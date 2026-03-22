import datetime as dt
import logging

import polars as pl
import requests

import pyield._internal.converters as cv
from pyield import bday
from pyield._internal.br_numbers import taxa_br
from pyield._internal.cache import ttl_cache
from pyield._internal.types import DateLike, any_is_empty

VERSAO_API = "1.0018"
URL_BASE = (
    f"https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/{VERSAO_API}"
)

URL_PAGINA_INICIAL = f"{URL_BASE}/taxasOnline.asp"
URL_CONSULTA_DADOS = f"{URL_BASE}/exibedados.asp"
URL_DOWNLOAD = f"{URL_BASE}/download_dados.asp?extensao=csv"

logger = logging.getLogger(__name__)


@ttl_cache()
def _buscar_dados_url(data_referencia: str) -> bytes:
    cabecalhos = {
        "Referer": URL_PAGINA_INICIAL,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",  # noqa
        "Origin": "https://www.anbima.com.br",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    carga = {
        "dataref": data_referencia,
        "dtRefIdioma": data_referencia,
        "layoutimprimir": "0",
        "idioma": "",
        "idresumo": "",
        "nome": "",
        "provedor": "",
        "codigo": "",
        "vencimento": "",
        "referencia": "",
        "dbNome": "",
        "fldColunas": ["C2", "C17", "C18", "C19"],
    }

    # Timeouts (conexão, leitura) ajustados ao perfil de cada request:
    # - GET inicial: página leve, apenas para estabelecer cookies de sessão.
    # - POST consulta: dispara a query no servidor (~30s observado em uso normal).
    # - POST download: dados já preparados pelo POST anterior, resposta rápida.
    timeout_pagina = (10, 20)
    timeout_consulta = (10, 120)
    timeout_download = (10, 30)

    with requests.Session() as s:
        s.get(URL_PAGINA_INICIAL, headers=cabecalhos, timeout=timeout_pagina)
        try:
            response_consulta = s.post(
                URL_CONSULTA_DADOS,
                headers=cabecalhos,
                data=carga,
                timeout=timeout_consulta,
            )
            response_consulta.raise_for_status()
            response_download = s.post(
                URL_DOWNLOAD,
                headers=cabecalhos,
                data=carga,
                timeout=timeout_download,
            )
            response_download.raise_for_status()
            if "text/html" in response_download.headers.get("Content-Type", ""):
                raise ValueError(
                    "Servidor respondeu com HTML em vez de CSV para a data "
                    f"{data_referencia}."
                )
            response_download.encoding = "iso-8859-1"
            return response_download.text.encode("utf-8")
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar dados para a data '{data_referencia}': {e}")
            raise


def _ler_csv_bruto(csv_bruto: bytes) -> pl.DataFrame:
    """Lê o CSV bruto da Anbima e devolve DataFrame com todas as colunas string."""
    return pl.read_csv(
        csv_bruto,
        separator=";",
        infer_schema=False,
        skip_rows=1,
    ).with_columns(pl.all().str.strip_chars())


def _processar_csv(data_ref: dt.date, df: pl.DataFrame) -> pl.DataFrame:
    """Converte o DataFrame bruto em DataFrame Polars estruturado."""

    return (
        df.with_columns(
            data_vencimento=pl.col("Vencimento").str.to_date(format="%d/%m/%Y"),
            horario=pl.when(pl.col("Provedor") == "ANBIMA 12H")
            .then(dt.time(12, 0))
            .otherwise(pl.col("Horário").str.to_time(format="%H:%M:%S")),
            taxa_venda=taxa_br("Oferta Venda"),
            taxa_compra=taxa_br("Oferta Compra"),
        )
        .select(
            data_hora_referencia=pl.lit(data_ref).dt.combine(pl.col("horario")),
            provedor=pl.col("Provedor"),
            titulo=pl.col("Título"),
            data_vencimento=pl.col("data_vencimento"),
            codigo_isin=pl.col("Código ISIN"),
            dias_uteis=bday.count_expr(data_ref, "data_vencimento"),
            taxa_indicativa_anterior=taxa_br("Fech D-1"),
            taxa_indicativa_superior=taxa_br("Indicativo Superior"),
            taxa_indicativa_inferior=taxa_br("Indicativo Inferior"),
            taxa_venda=pl.col("taxa_venda"),
            taxa_compra=pl.col("taxa_compra"),
            taxa_media=pl.mean_horizontal("taxa_compra", "taxa_venda"),
            taxa_ultima=taxa_br("Última"),
        )
        .sort("titulo", "data_vencimento", "data_hora_referencia")
    )


def tpf_difusao(data_referencia: DateLike) -> pl.DataFrame:
    """
    Obtém a TPF Difusão da Anbima para uma data de referência específica.

    Args:
        data_referencia (DateLike): Data de referência.

    Returns:
        pl.DataFrame: DataFrame com os dados.

    Raises:
        requests.RequestException: Em falhas operacionais de rede ou HTTP.
        ValueError: Em resposta inválida da API ou erro de parsing do CSV.

    Output Columns:
        - data_hora_referencia (Datetime): Data e hora de referência da taxa.
        - provedor (string): Provedor dos dados.
        - titulo (string): Nome do título (ex: LFT, LTN).
        - data_vencimento (date): Data de vencimento do título.
        - codigo_isin (string): código ISIN do título.
        - dias_uteis (int): Dias úteis entre a data de referência e o vencimento.
        - taxa_indicativa_anterior (float): Taxa indicativa de fechamento D-1 (decimal).
        - taxa_indicativa_superior (float): Limite superior da banda indicativa (decimal).
        - taxa_indicativa_inferior (float): Limite inferior da banda indicativa (decimal).
        - taxa_venda (float): Taxa de oferta de venda (Ask rate) (decimal).
        - taxa_compra (float): Taxa de oferta de compra (Bid rate) (decimal).
        - taxa_media (float): Média entre a taxa de compra e venda (decimal).
        - taxa_ultima (float): Última taxa negociada (decimal).
    """
    if any_is_empty(data_referencia):
        return pl.DataFrame()
    data = cv.converter_datas(data_referencia)
    if not bday.is_business_day(data):
        return pl.DataFrame()
    data_str = data.strftime("%d/%m/%Y")
    csv_bruto = _buscar_dados_url(data_str)
    df = _ler_csv_bruto(csv_bruto)
    return _processar_csv(data, df)
