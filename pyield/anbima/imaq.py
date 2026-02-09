"""
Exemplo de página HTML:
    Título Codigo Selic  Código ISIN Data de Vencimento Quantidade em Mercado (1.000 Títulos)    PU (R$) Valor de Mercado (R$ Mil)  Variação da Quantidade  (1.000 Títulos)        Status do Titulo
       LTN       100000 BRSTNCLTN863         01/10/2025                           115.870,772 997,241543               115.551.147                                    0,000 Participante Definitivo
       LTN       100000 BRSTNCLTN7U7         01/01/2026                           176.807,732 963,001853               170.266.174                                   -1,987 Participante Definitivo
       LTN       100000 BRSTNCLTN8B5         01/04/2026                           115.826,847 931,607124               107.905.116                                    0,000 Participante Definitivo
"""  # noqa

import datetime as dt
import logging
import re

import polars as pl
import polars.selectors as ps
import requests
from lxml.html import HTMLParser
from lxml.html import fromstring as html_fromstring

import pyield.converters as cv
from pyield.anbima.tpf import tpf_data
from pyield.types import DateLike, any_is_empty

logger = logging.getLogger(__name__)

URL_IMA = "https://www.anbima.com.br/informacoes/ima/ima-quantidade-mercado.asp"

MAPA_COLUNAS = {
    "Título": ("BondType", pl.String),
    "Codigo Selic": ("SelicCode", pl.Int64),
    "Código ISIN": ("ISIN", pl.String),
    "Data de Vencimento": ("MaturityDate", pl.String),
    "Quantidade em Mercado (1.000 Títulos)": ("MarketQuantity", pl.Float64),
    "PU (R$)": ("Price", pl.Float64),
    "Valor de Mercado (R$ Mil)": ("MarketValue", pl.Float64),
    "Variação da Quantidade (1.000 Títulos)": ("QuantityVariation", pl.Float64),
    "Status do Titulo": ("BondStatus", pl.String),
}

ALIAS_COLUNAS = {col: alias for col, (alias, _) in MAPA_COLUNAS.items()}
ESQUEMA_DADOS = {alias: dtype for _, (alias, dtype) in MAPA_COLUNAS.items()}

COLUNAS_INT = [
    "MarketQuantity",
    "MarketValue",
    "QuantityVariation",
    "MarketDV01",
    "MarketDV01USD",
]

ORDEM_COLUNAS_FINAL = [
    "Date",
    "BondType",
    "MaturityDate",
    "SelicCode",
    "ISIN",
    "Price",
    "MarketQuantity",
    "MarketDV01",
    "MarketDV01USD",
    "MarketValue",
    "QuantityVariation",
    "BondStatus",
]


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


def _extrair_data_referencia(html_content: bytes) -> dt.date | None:
    """Extrai a data de referência a partir do HTML.

    Returns:
        A data de referência encontrada ou None se não for possível identificar.
    """
    padrao_data = r"\b(\d{2}/\d{2}/\d{4})\b"
    match = re.search(padrao_data, html_content.decode("iso-8859-1"))

    if not match:
        return None

    data_str = match.group(1)
    return dt.datetime.strptime(data_str, "%d/%m/%Y").date()


def _normalizar_nome_coluna(texto: str) -> str:
    """Normaliza cabeçalhos removendo quebras de linha e espaços extras."""
    return " ".join(texto.strip().split())


def _parsear_valor_celula(texto: str) -> str:
    """
    Converte valor de célula do formato brasileiro para o padrão.

    Formato brasileiro: 129.253,568 -> 129253.568
    Valores ausentes (--) retornam string vazia.
    """
    texto = texto.strip()

    if texto == "--" or not texto:
        return ""

    # Convert Brazilian number format
    if "," in texto or "." in texto:
        if any(c.isdigit() for c in texto):
            texto = texto.replace(".", "")  # Remove separador de milhar
            texto = texto.replace(",", ".")  # Substitui separador decimal

    return texto


def _parsear_tabelas_html(html_content: bytes) -> pl.DataFrame:
    """Parseia tabelas HTML com lxml e retorna DataFrame (colunas String).

    Extrai dados das tabelas aninhadas (com parent::td),
    converte formato numérico brasileiro e retorna DataFrame bruto.
    """
    html_content = html_content.replace(b"<br>", b" ").replace(b"<BR>", b" ")

    parser = HTMLParser(encoding="iso-8859-1")
    tree = html_fromstring(html_content, parser=parser)

    nested_tables = tree.xpath("//table[@width='100%'][parent::td]")

    dados = []
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
            dados.append([_parsear_valor_celula(c.text_content()) for c in cells])

    if not dados or not nomes_colunas:
        return pl.DataFrame()

    return pl.DataFrame(dados, schema=nomes_colunas, orient="row")


def _processar_df(df: pl.DataFrame, data_referencia: dt.date) -> pl.DataFrame:
    """Renomeia, filtra, converte tipos e aplica transformações numéricas."""
    return (
        df.rename(ALIAS_COLUNAS)
        # Strip whitespace e converte strings vazias em null
        .with_columns(ps.string().str.strip_chars().name.keep())
        .with_columns(
            pl.when(ps.string().str.len_chars() == 0)
            .then(None)
            .otherwise(ps.string())
            .name.keep()
        )
        .filter(
            pl.col("MaturityDate").is_not_null(),
            pl.col("BondType") != "Título",
        )
        .unique(subset="ISIN")
        .cast(ESQUEMA_DADOS)
        .with_columns(
            pl.col("MaturityDate").str.to_date(format="%d/%m/%Y"),
            pl.col("MarketQuantity") * 1000,
            pl.col("MarketValue") * 1000,
            pl.col("QuantityVariation") * 1000,
            Date=data_referencia,
        )
        .sort("BondType", "MaturityDate")
    )


def _adicionar_dv01(df: pl.DataFrame, data_referencia: dt.date) -> pl.DataFrame:
    df_anbima = tpf_data(data_referencia)
    colunas_manter = ["ReferenceDate", "BondType", "MaturityDate", "DV01", "DV01USD"]
    df_anbima = df_anbima.select(colunas_manter).rename({"ReferenceDate": "Date"})
    # Guard clause for missing columns
    if "DV01" not in df_anbima.columns or "DV01USD" not in df_anbima.columns:
        return df

    df = df.join(df_anbima, on=["Date", "BondType", "MaturityDate"], how="left")
    # Calcular os estoques
    df = df.with_columns(
        MarketDV01=pl.col("DV01") * pl.col("MarketQuantity"),
        MarketDV01USD=pl.col("DV01USD") * pl.col("MarketQuantity"),
    ).drop("DV01", "DV01USD")
    return df


def _finalizar(df: pl.DataFrame) -> pl.DataFrame:
    """Converte colunas inteiras e reordena colunas para saída final."""
    return df.with_columns(pl.col(COLUNAS_INT).round(0).cast(pl.Int64)).select(
        ORDEM_COLUNAS_FINAL
    )


def imaq(date: DateLike) -> pl.DataFrame:
    """Consulta e processa dados de estoque IMA-Q da ANBIMA para uma data.

    Args:
        date: Data de referência. Apenas os últimos 5 dias úteis estão
            disponíveis; o mais recente é tipicamente 2 dias úteis atrás.

    Returns:
        DataFrame com dados processados. Em caso de erro retorna DataFrame
        vazio e registra log da exceção.

    Output Columns:
        * Date (Date): data de referência dos dados.
        * BondType (String): tipo do título (LTN, NTN-B, NTN-F, LFT, …).
        * MaturityDate (Date): data de vencimento do título.
        * SelicCode (Int64): código SELIC do título.
        * ISIN (String): código ISIN (International Securities Id Number).
        * Price (Float64): PU do título em R$.
        * MarketQuantity (Int64): quantidade em mercado (unidades).
        * MarketDV01 (Int64): DV01 do estoque em R$.
        * MarketDV01USD (Int64): DV01 do estoque em USD.
        * MarketValue (Int64): valor de mercado em R$.
        * QuantityVariation (Int64): variação diária da quantidade.
        * BondStatus (String): status do título.

    Notes:
        - Valores convertidos para unidades puras (ex: MarketQuantity × 1.000).
        - DV01 obtidos via cruzamento com tpf_data(); nulos se indisponível.

    Examples:
        >>> from pyield import bday
        >>> data_ref = bday.offset(bday.last_business_day(), -2)
        >>> df = imaq(data_ref)
        >>> df["Date"].first() == data_ref
        True
    """
    if any_is_empty(date):
        logger.warning("Nenhuma data informada. Retornando DataFrame vazio.")
        return pl.DataFrame()
    data = cv.converter_datas(date)
    data_str = data.strftime("%d/%m/%Y")
    try:
        url_content = _buscar_conteudo_url(data)
        if not url_content:
            logger.warning(
                f"Sem dados disponíveis para {data_str}. Retornando DataFrame vazio."
            )
            return pl.DataFrame()

        # ✅ VALIDAÇÃO CRÍTICA EXPLÍCITA NO FLUXO PRINCIPAL
        data_referencia = _extrair_data_referencia(url_content)

        if data_referencia is None:
            raise ValueError(
                f"Não foi possível encontrar data de referência no HTML para {data_str}"
            )

        if data_referencia != data:
            raise ValueError(
                f"Data de referência divergente: esperado {data_str}, "
                f"encontrado {data_referencia.strftime('%d/%m/%Y')}"
            )

        df = _parsear_tabelas_html(url_content)
        if df.is_empty():
            return pl.DataFrame()
        df = _processar_df(df, data)
        df = _adicionar_dv01(df, data)
        return _finalizar(df)
    except Exception:  # Erro inesperado
        msg = f"Erro ao buscar IMA para {data_str}. Retornando DataFrame vazio."
        logger.exception(msg)
        return pl.DataFrame()
