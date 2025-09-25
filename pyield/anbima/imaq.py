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
import requests

from pyield import date_converter as dc
from pyield.anbima.tpf import tpf_data
from pyield.date_converter import DateScalar

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


def _get_reference_date(html_content: bytes) -> dt.date | None:
    # Expressão regular para datas no formato dd/mm/ano
    date_pattern = r"\b(\d{2}/\d{2}/\d{4})\b"

    # Busca pela primeira data
    match = re.search(date_pattern, html_content.decode("iso-8859-1"))

    if match:
        anbima_date_str = match.group(1)
        anbima_date = pd.to_datetime(anbima_date_str, format="%d/%m/%Y").date()
        return anbima_date
    return None


def _read_html_data(html_content: str) -> pd.DataFrame:
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
    return pd.concat(dfs).astype("string[pyarrow]")


def _prepare_df(df: pd.DataFrame, reference_date: dt.date) -> pd.DataFrame:
    df = (
        pl.from_pandas(df)
        .filter(
            pl.col("Data de Vencimento").is_not_null(),
            (pl.col("Título") != "Título"),
        )
        .unique(subset="Código ISIN")
        .rename(COLUMN_MAPPING)
        .with_columns(
            pl.col("BondType").str.strip_chars(),
            pl.col("SelicCode").cast(pl.Int64),
            pl.col("ISIN").str.strip_chars(),
            pl.col("MaturityDate").str.strptime(pl.Date, format="%d/%m/%Y"),
            pl.col("MarketQuantity").cast(pl.Float64),
            pl.col("Price").cast(pl.Float64),
            pl.col("MarketValue").cast(pl.Float64),
            pl.col("QuantityVariation").cast(pl.Float64),
            pl.col("BondStatus").str.strip_chars(),
            Date=reference_date,
        )
    )

    return df.to_pandas(use_pyarrow_extension_array=True)


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["MarketQuantity", "MarketValue", "QuantityVariation"]:
        # Remove the thousands unit from numeric columns
        df[col] = (1000 * df[col]).round(0).astype("int64[pyarrow]")

    return df.sort_values(by=["BondType", "MaturityDate"]).reset_index(drop=True)


def _add_dv01(df: pd.DataFrame) -> pd.DataFrame:
    target_date = df["Date"].min()
    df_anbima = tpf_data(target_date)
    target_cols = ["ReferenceDate", "BondType", "MaturityDate", "DV01", "DV01USD"]
    df_anbima = df_anbima[target_cols].rename(columns={"ReferenceDate": "Date"})
    # Guard clause for missing columns
    if "DV01" not in df_anbima.columns or "DV01USD" not in df_anbima.columns:
        return df

    df = df.merge(df_anbima, on=["Date", "BondType", "MaturityDate"], how="left")
    # Calcular os estoques
    df["MarketDV01"] = df["DV01"] * df["MarketQuantity"]
    df["MarketDV01USD"] = df["DV01USD"] * df["MarketQuantity"]
    for col in ["MarketDV01", "MarketDV01USD"]:
        df[col] = df[col].round(0).astype("int64[pyarrow]")
    # Remover colunas desnecessárias
    df = df.drop(columns=["DV01", "DV01USD"], errors="ignore")
    return df


def _reorder_df(df: pd.DataFrame) -> pd.DataFrame:
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
    return df[column_order].copy()


def imaq(date: DateScalar) -> pd.DataFrame:
    """
    Fetch and process IMA market data for a given date.

    This function retrieves IMA quantity market data from ANBIMA for a given date,
    processes the data into a structured DataFrame, and returns the resulting DataFrame.
    It handles conversion of date formats, renames columns to English, and converts
    certain numeric columns to integer types. In the event of an error during data
    fetching or processing, an empty DataFrame is returned.

    Args:
        date (DateScalar): A date-like object representing the target date for fetching
            the data.

    Returns:
        pd.DataFrame: A DataFrame containing the IMA data.

    DataFrame columns:
        - Date: Reference date of the data.
        - BondType: Type of bond.
        - Maturity: Bond maturity date.
        - SelicCode: Code representing the SELIC rate.
        - ISIN: International Securities Identification Number.
        - Price: Bond price.
        - MarketQuantity: Market quantity (in units of 1000 bonds).
        - MarketValue: Market value (in units of 1000 reais).
        - QuantityVariation: Variation in quantity (in units of 1000 bonds).
        - BondStatus: Status of the bond.

    Raises:
        Exception: Logs error and returns an empty DataFrame if any error occurs during
            fetching or processing.

    """
    date = dc.convert_input_dates(date)
    date_str = date.strftime("%d/%m/%Y")
    try:
        url_content = _fetch_url_content(date)
        if not url_content:
            logger.warning(
                f"No data available for {date_str}. Returning an empty DataFrame."
            )
            return pd.DataFrame()

        reference_date = _get_reference_date(url_content)
        if not reference_date:
            logger.warning(
                f"Could not determine reference date for {date_str}. "
                "Returning an empty DataFrame."
            )
            return pd.DataFrame()

        df = _read_html_data(url_content)
        df = _prepare_df(df, reference_date)

        df = _process_df(df)
        df = _add_dv01(df)
        df = _reorder_df(df)
        return df
    except Exception:  # Erro inesperado
        msg = f"Error fetching IMA for {date_str}. Returning empty DataFrame."
        logger.exception(msg)
        return pd.DataFrame()
