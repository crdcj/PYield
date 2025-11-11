"""
HTML page example:
    Título Codigo Selic  Código ISIN Data de Vencimento Quantidade em Mercado (1.000 Títulos)    PU (R$) Valor de Mercado (R$ Mil)  Variação da Quantidade  (1.000 Títulos)        Status do Titulo
       LTN       100000 BRSTNCLTN863         01/10/2025                           115.870,772 997,241543               115.551.147                                    0,000 Participante Definitivo
       LTN       100000 BRSTNCLTN7U7         01/01/2026                           176.807,732 963,001853               170.266.174                                   -1,987 Participante Definitivo
       LTN       100000 BRSTNCLTN8B5         01/04/2026                           115.826,847 931,607124               107.905.116                                    0,000 Participante Definitivo
"""  # noqa

import datetime as dt
import io
import logging
import re

import pandas as pd
import polars as pl
import polars.selectors as ps
import requests

import pyield.converters as cv
from pyield.anbima.tpf import tpf_data
from pyield.types import DateLike, has_nullable_args

# Configura o logger do módulo
logger = logging.getLogger(__name__)

# --- Configurações Centralizadas ---
IMA_URL = "https://www.anbima.com.br/informacoes/ima/ima-quantidade-mercado.asp"
COLUMN_MAPPING = {
    "Título": "BondType",
    "Codigo Selic": "SelicCode",
    "Código ISIN": "ISIN",
    "Data de Vencimento": "MaturityDate",
    "Quantidade em Mercado (1.000 Títulos)": "MarketQuantity",
    "PU (R$)": "Price",
    "Valor de Mercado (R$ Mil)": "MarketValue",
    "Variação da Quantidade (1.000 Títulos)": "QuantityVariation",
    "Status do Titulo": "BondStatus",
}


def _fetch_url_content(target_date: dt.date) -> bytes:
    target_date_str = target_date.strftime("%d/%m/%Y")
    payload = {
        "Tipo": "",
        "DataRef": "",
        "Pai": "ima",
        "Dt_Ref_Ver": "20250117",
        "Dt_Ref": f"{target_date_str}",
    }

    r = requests.post(IMA_URL, data=payload, timeout=10)
    r.raise_for_status()
    if "Não há dados disponíveis" in r.text:
        return ""
    return r.content


def _extract_reference_date(html_content: bytes) -> dt.date | None:
    """
    Extract reference date from HTML content.

    Returns:
        The reference date found in the HTML, or None if not found.
    """
    date_pattern = r"\b(\d{2}/\d{2}/\d{4})\b"
    match = re.search(date_pattern, html_content.decode("iso-8859-1"))

    if not match:
        return None

    date_string = match.group(1)
    return dt.datetime.strptime(date_string, "%d/%m/%Y").date()


def _parse_html_data(html_content: str) -> str:
    dfs = pd.read_html(
        io.BytesIO(html_content),
        flavor="lxml",
        attrs={"width": "100%"},
        header=0,
        thousands=".",
        decimal=",",
        displayed_only=True,
        dtype_backend="pyarrow",
        na_values="--",
        encoding="iso-8859-1",
    )
    return pd.concat(dfs).to_csv(index=False)


def _pre_process_data(raw_csv: str) -> str:
    csv = (
        pl.read_csv(io.StringIO(raw_csv))
        .rename(COLUMN_MAPPING)
        .with_columns(ps.string().str.strip_chars().name.keep())
        .filter(
            pl.col("MaturityDate").is_not_null(),
            pl.col("BondType") != "Título",
        )
        .unique(subset="ISIN")
        .write_csv()
    )
    return csv


def _process_data(csv: str, reference_date: dt.date) -> pl.DataFrame:
    df = (
        pl.read_csv(io.StringIO(csv))
        .with_columns(
            pl.col("MaturityDate").str.strptime(pl.Date, format="%d/%m/%Y"),
            (pl.col("MarketQuantity") * 1000),
            (pl.col("MarketValue") * 1000),
            (pl.col("QuantityVariation") * 1000),
            Date=reference_date,
        )
        .sort(by=["BondType", "MaturityDate"])
    )

    return df


def _add_dv01(df: pl.DataFrame, reference_date: dt.date) -> pl.DataFrame:
    df_anbima = tpf_data(reference_date)
    target_cols = ["ReferenceDate", "BondType", "MaturityDate", "DV01", "DV01USD"]
    df_anbima = df_anbima.select(target_cols).rename({"ReferenceDate": "Date"})
    # Guard clause for missing columns
    if "DV01" not in df_anbima.columns or "DV01USD" not in df_anbima.columns:
        return df

    df = df.join(df_anbima, on=["Date", "BondType", "MaturityDate"], how="left")
    # Calcular os estoques
    df = df.with_columns(
        MarketDV01=pl.col("DV01") * pl.col("MarketQuantity"),
        MarketDV01USD=pl.col("DV01USD") * pl.col("MarketQuantity"),
    ).drop(["DV01", "DV01USD"])
    return df


def _cast_int_columns(df: pl.DataFrame) -> pl.DataFrame:
    integer_cols = [
        "MarketQuantity",
        "MarketValue",
        "QuantityVariation",
        "MarketDV01",
        "MarketDV01USD",
    ]
    df = df.with_columns(pl.col(integer_cols).round(0).cast(pl.Int64))
    return df


def _reorder_df(df: pl.DataFrame) -> pl.DataFrame:
    # Reorder the DataFrame columns
    column_order = [
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
    return df.select(column_order)


def imaq(date: DateLike) -> pl.DataFrame:
    """
    Fetch and process IMA market data for a given date.

    This function retrieves IMA quantity market data from ANBIMA for a given date,
    processes the data into a structured DataFrame, and returns the resulting DataFrame.
    It handles conversion of date formats, renames columns to English, and converts
    certain numeric columns to integer types. In the event of an error during data
    fetching or processing, an empty DataFrame is returned.

    Args:
        date (DateLike): A date-like object representing the target date for fetching
            the data. Only 5 business days from the last available data are available.
            The last is typically from 2 business days ago.

    Returns:
        pl.DataFrame: A DataFrame containing the IMA data.

    DataFrame columns:
        - Date: Reference date of the data.
        - BondType: Type of bond.
        - Maturity: Bond maturity date.
        - SelicCode: Code representing the SELIC rate.
        - ISIN: International Securities Identification Number.
        - Price: Bond price.
        - MarketQuantity: Market quantity .
        - MarketDV01: Market DV01 .
        - MarketDV01USD: Market DV01 in USD.
        - MarketValue: Market value .
        - QuantityVariation: Variation in quantity .
        - BondStatus: Status of the bond.

    Notes:
        - Values are converted to pure units (e.g., MarketQuantity multiplied by 1,000).

    Raises:
        Exception: Logs error and returns an empty DataFrame if any error occurs during
            fetching or processing.

    Examples:
        >>> from pyield import bday
        >>> target_date = bday.offset(dt.date.today(), -5)
        >>> df = imaq(target_date)
        >>> df["Date"].first() == target_date
        True
    """
    if has_nullable_args(date):
        logger.warning("No date provided. Returning empty DataFrame.")
        return pl.DataFrame()
    date = cv.convert_dates(date)
    date_str = date.strftime("%d/%m/%Y")
    try:
        url_content = _fetch_url_content(date)
        if not url_content:
            logger.warning(
                f"No data available for {date_str}. Returning an empty DataFrame."
            )
            return pl.DataFrame()

        # ✅ VALIDAÇÃO CRÍTICA EXPLÍCITA NO FLUXO PRINCIPAL
        reference_date = _extract_reference_date(url_content)

        if reference_date is None:
            raise ValueError(f"No reference date found in HTML for {date_str}")

        if reference_date != date:
            raise ValueError(
                f"Reference date mismatch: expected {date_str}, "
                f"got {reference_date.strftime('%d/%m/%Y')}"
            )

        raw_csv = _parse_html_data(url_content)
        clean_csv = _pre_process_data(raw_csv)
        df = _process_data(clean_csv, date)
        df = _add_dv01(df, date)
        df = _cast_int_columns(df)
        df = _reorder_df(df)
        return df
    except Exception:  # Erro inesperado
        msg = f"Error fetching IMA for {date_str}. Returning empty DataFrame."
        logger.exception(msg)
        return pl.DataFrame()
