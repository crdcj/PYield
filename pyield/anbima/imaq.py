"""
Exemplo de página HTML:
    Título Codigo Selic  Código ISIN Data de Vencimento Quantidade em Mercado (1.000 Títulos)    PU (R$) Valor de Mercado (R$ Mil)  Variação da Quantidade  (1.000 Títulos)        Status do Titulo
       LTN       100000 BRSTNCLTN863         01/10/2025                           115.870,772 997,241543               115.551.147                                    0,000 Participante Definitivo
       LTN       100000 BRSTNCLTN7U7         01/01/2026                           176.807,732 963,001853               170.266.174                                   -1,987 Participante Definitivo
       LTN       100000 BRSTNCLTN8B5         01/04/2026                           115.826,847 931,607124               107.905.116                                    0,000 Participante Definitivo
"""  # noqa

import datetime as dt

import polars as pl
import polars.selectors as ps
import requests
from lxml.html import HTMLParser
from lxml.html import fromstring as html_fromstring

import pyield._internal.converters as cv
from pyield._internal.br_numbers import float_br, inteiro_m
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike

URL_IMA = "https://www.anbima.com.br/informacoes/ima/ima-quantidade-mercado.asp"


@ttl_cache()
@retry_padrao
def _buscar_conteudo_url(data_referencia: dt.date) -> bytes:
    data_referencia_str = data_referencia.strftime("%d/%m/%Y")
    payload = {
        "Tipo": "",
        "DataRef": "",
        "Pai": "ima",
        "Dt_Ref_Ver": "20250117",
        "Dt_Ref": f"{data_referencia_str}",
    }

    resposta = requests.post(URL_IMA, data=payload, timeout=10)
    resposta.raise_for_status()
    if "Não há dados disponíveis" in resposta.text:
        return b""
    return resposta.content


def _normalizar_nome_coluna(texto: str) -> str:
    """Normaliza cabeçalhos removendo quebras de linha e espaços extras."""
    return " ".join(texto.strip().split())


def _parsear_tabelas_html(html_content: bytes) -> pl.DataFrame:
    """Parseia tabelas HTML com lxml e retorna DataFrame via read_csv.

    Extrai dados das tabelas aninhadas (com parent::td),
    converte para CSV (separador tab) e lê com Polars.
    """
    html_content = html_content.replace(b"<br>", b" ").replace(b"<BR>", b" ")

    parser = HTMLParser(encoding="iso-8859-1")
    tree = html_fromstring(html_content, parser=parser)

    nested_tables = tree.xpath("//table[@width='100%'][parent::td]")

    linhas = []
    nomes_colunas = None

    for table in nested_tables:  # type: ignore[misc]
        headers = table.xpath(".//thead//th")
        if not nomes_colunas:
            nomes_colunas = [_normalizar_nome_coluna(h.text_content()) for h in headers]

        data_rows = table.xpath(".//tbody//tr[td]")
        for row in data_rows:
            cells = row.xpath(".//td")
            if len(cells) != len(nomes_colunas):
                continue
            linhas.append("\t".join(c.text_content().strip() for c in cells))

    if not linhas or not nomes_colunas:
        return pl.DataFrame()

    cabecalho = "\t".join(nomes_colunas)
    csv_bytes = ("\n".join([cabecalho, *linhas])).encode("utf-8")
    return pl.read_csv(
        csv_bytes,
        separator="\t",
        infer_schema=False,
        null_values="--",
    )


def _processar_df(df: pl.DataFrame, data_referencia: dt.date) -> pl.DataFrame:
    """Filtra, converte tipos e aplica transformações numéricas."""
    return (
        df.with_columns(ps.string().str.strip_chars().name.keep())
        .filter(
            pl.col("Data de Vencimento").is_not_null(),
            pl.col("Título") != "Título",
        )
        .unique(subset="Código ISIN")
        .select(
            data_referencia=data_referencia,
            titulo=pl.col("Título"),
            data_vencimento=pl.col("Data de Vencimento").str.to_date(format="%d/%m/%Y"),
            codigo_selic=pl.col("Codigo Selic").cast(pl.Int64),
            isin=pl.col("Código ISIN"),
            pu=float_br("PU (R$)"),
            quantidade_mercado=inteiro_m("Quantidade em Mercado (1.000 Títulos)"),
            valor_mercado=inteiro_m("Valor de Mercado (R$ Mil)"),
            variacao_quantidade=inteiro_m("Variação da Quantidade (1.000 Títulos)"),
            status_titulo=pl.col("Status do Titulo"),
        )
        .sort("titulo", "data_vencimento")
    )


def imaq(data: DateLike) -> pl.DataFrame:
    """Busca estoque IMA-Q na camada técnica da ANBIMA.

    Use ``pyield.tpf.estoque`` na API pública principal.
    """
    data = cv.converter_datas(data)
    if not cv.data_referencia_valida(data):
        return pl.DataFrame()

    url_content = _buscar_conteudo_url(data)
    if not url_content:
        return pl.DataFrame()

    df = _parsear_tabelas_html(url_content)
    if df.is_empty():
        return pl.DataFrame()
    return _processar_df(df, data)
