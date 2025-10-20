import datetime as dt
import logging
from io import StringIO

import polars as pl
import requests

from pyield.retry import default_retry

logger = logging.getLogger(__name__)
LAST_ETTJ_URL = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"
INTRADAY_ETTJ_URL = (
    "https://www.anbima.com.br/informacoes/curvas-intradiarias/cIntra-down.asp"
)

# Anbima ETTJ data has 4 decimal places in percentage values
# We will round to 6 decimal places to avoid floating point errors
ROUND_DIGITS = 6


@default_retry
def _get_last_content_text() -> str:
    """Fetches the raw yield curve data from ANBIMA."""
    request_payload = {
        "Idioma": "PT",
        "Dt_Ref": "",
        "saida": "csv",
    }
    r = requests.post(LAST_ETTJ_URL, data=request_payload)
    r.raise_for_status()
    r.encoding = "latin1"
    return r.text


def _get_reference_date(text: str) -> dt.date:
    file_date_str = text[0:10]  # formato = 09/09/2024
    file_date = dt.datetime.strptime(file_date_str, "%d/%m/%Y").date()
    return file_date


def _filter_ettf_text(texto_completo: str) -> str:
    # Definir os marcadores de início e fim
    marcador_inicio = "Vertices;ETTJ IPCA;ETTJ PREF;Inflação Implícita"
    marcador_fim = "PREFIXADOS (CIRCULAR 3.361)"

    # 2. Dividir o texto em uma lista de linhas
    linhas = texto_completo.strip().splitlines()

    # 3. Encontrar os índices das linhas de início e fim
    indice_inicio = linhas.index(marcador_inicio)
    indice_fim = linhas.index(marcador_fim)

    # 4. Fatiar a lista para extrair o trecho desejado
    trecho_filtrado = linhas[indice_inicio:indice_fim]

    # Remover linhas vazias que possam ter sido incluídas no final
    while trecho_filtrado and not trecho_filtrado[-1].strip():
        trecho_filtrado.pop()

    # 5. Juntar as linhas filtradas em um único texto e retornar
    return "\n".join(trecho_filtrado).replace(".", "").replace(",", ".")


def _convert_text_to_df(text: str, reference_date: dt.date) -> pl.DataFrame:
    """Converts the raw yield curve text to a Polars DataFrame."""
    df = pl.read_csv(
        StringIO(text),
        separator=";",
    ).with_columns(pl.lit(reference_date).alias("date"))
    return df


def _process_df(df: pl.DataFrame) -> pl.DataFrame:
    """Processes the raw yield curve DataFrame to calculate rates and forward rates."""
    # Rename columns
    rename_dict = {
        "Vertices": "vertex",
        "ETTJ IPCA": "real_rate",
        "ETTJ PREF": "nominal_rate",
        "Inflação Implícita": "implied_inflation",
    }
    df = df.rename(rename_dict).with_columns(
        (pl.col("real_rate") / 100).round(ROUND_DIGITS),
        (pl.col("nominal_rate") / 100).round(ROUND_DIGITS),
        (pl.col("implied_inflation") / 100).round(ROUND_DIGITS),
    )
    column_order = [
        "date",
        "vertex",
        "nominal_rate",
        "real_rate",
        "implied_inflation",
    ]
    return df.select(column_order)


def last_ettj() -> pl.DataFrame:
    """
    Retrieves and processes the latest Brazilian yield curve data from ANBIMA.

    This function fetches the most recent yield curve data published by ANBIMA,
    containing real rates (IPCA-indexed), nominal rates, and implied inflation
    at various vertices (time points).

    Returns:
        pl.DataFrame: A DataFrame containing the latest ETTJ data.

    DataFrame columns:
        - date: Reference date of the yield curve
        - vertex: Time point in business days
        - nominal_rate: Zero-coupon nominal interest rate
        - real_rate: Zero-coupon real interest rate (IPCA-indexed)
        - implied_inflation: Implied inflation rate (break-even inflation)

    Note:
        All rates are expressed in decimal format (e.g., 0.12 for 12%).
    """
    text = _get_last_content_text()
    reference_date = _get_reference_date(text)
    text = _filter_ettf_text(text)
    df = _convert_text_to_df(text, reference_date)
    df = _process_df(df)
    return df
