"""
Fetches real-time secondary trading data for domestic Federal Public Debt (FPD)
https://www.bcb.gov.br/htms/selic/selicprecos.asp?frame=1

CSV file example:

"""

import datetime as dt
import io
import logging
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from polars import selectors as cs

from pyield import bday
from pyield.config import TIMEZONE_BZ

REALTIME_START_TIME = dt.time(9, 0, 0)
REALTIME_END_TIME = dt.time(22, 0, 0)

logger = logging.getLogger(__name__)
test_file = Path(__file__).parent.parent.parent / "tests/data/negocios-registrados.csv"


def _fetch_csv_from_url() -> str:
    """
    Example URL for the CSV file containing intraday trades data:
        https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/02-06-2025
    """
    today = dt.datetime.now(TIMEZONE_BZ).date()
    formatted_date = today.strftime("%d-%m-%Y")
    FILE_URL = f"https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/{formatted_date}"
    r = requests.get(FILE_URL, timeout=30)  # API usually takes 10s to respond
    r.raise_for_status()
    r.encoding = "utf-8-sig"  # Handle BOM for UTF-8
    return r.text


def _clean_csv(text: str) -> str:
    """Clean the CSV text data by removing unnecessary rows."""
    rows = text.splitlines()
    header = rows[0]
    valid_rows = [header] + [row for row in rows if row.startswith("1;")]
    text = "\n".join(valid_rows)
    text = text.replace(".", "")  # Remove thousands separator
    text = text.replace(",", ".")  # Replace decimal comma with dot
    return text


def _convert_csv_to_df(text: str) -> pl.DataFrame:
    return pl.read_csv(
        io.StringIO(text),
        separator=";",
        null_values="-",
    )


def _process_df(df: pl.DataFrame) -> pl.DataFrame:
    col_mapping = {
        "//1": "RowType",
        "código título": "SelicCode",
        " data vencimento": "MaturityDate",
        " sigla": "BondType",
        " mercado à vista pu último": "LastPrice",
        " tx último": "LastRate",
        " pu mínimo": "MinPrice",
        " tx mínimo": "MinRate",
        " pu médio": "AvgPrice",
        " tx médio": "AvgRate",
        " pu máximo": "MaxPrice",
        " tx máximo": "MaxRate",
        " totais liquidados operações": "Trades",
        " corretagem liquidados operações": "BrokeredTrades",
        " títulos": "Quantity",
        " corretagem títulos": "BrokeredQuantity",
        " financeiro": "Value",
        " mercado a termo pu último": "ForwardLastPrice",
        " tx último_duplicated_0": "ForwardLastRate",
        " pu mínimo_duplicated_0": "ForwardMinPrice",
        " tx mínimo_duplicated_0": "ForwardMinRate",
        " pu médio_duplicated_0": "ForwardAvgPrice",
        " tx médio_duplicated_0": "ForwardAvgRate",
        " pu máximo_duplicated_0": "ForwardMaxPrice",
        " tx máximo_duplicated_0": "ForwardMaxRate",
        " totais contratados operações": "ForwardTrades",
        " corretagem contratados operações": "ForwardBrokeredTrades",
        " títulos_duplicated_0": "ForwardQuantity",
        " corretagem títulos_duplicated_0": "ForwardBrokeredQuantity",
        " financeiro_duplicated_0": "ForwardValue",
    }
    now = dt.datetime.now(TIMEZONE_BZ)
    today = now.date()
    df = (
        df.rename(col_mapping)
        .drop(["RowType"], strict=False)
        .with_columns(
            pl.col("BondType").str.strip_chars(),
            pl.col("MaturityDate").str.strptime(pl.Date, "%d/%m/%Y"),
            pl.lit(today).alias("SettlementDate"),
            pl.lit(now).alias("CollectedAt"),
            (cs.contains("Rate") / 100).round(6),
        )
    )
    return df


def _reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
    reorder_columns = [
        # "RowType",
        "CollectedAt",
        "SettlementDate",
        "BondType",
        "SelicCode",
        "MaturityDate",
        "MinPrice",
        "AvgPrice",
        "MaxPrice",
        "LastPrice",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "LastRate",
        "Trades",
        "Quantity",
        "Value",
        "BrokeredTrades",
        "BrokeredQuantity",
        "ForwardMinPrice",
        "ForwardAvgPrice",
        "ForwardLastPrice",
        "ForwardMaxPrice",
        "ForwardLastRate",
        "ForwardMinRate",
        "ForwardAvgRate",
        "ForwardMaxRate",
        "ForwardTrades",
        "ForwardQuantity",
        "ForwardValue",
        "ForwardBrokeredTrades",
        "ForwardBrokeredQuantity",
    ]

    column_order = [col for col in reorder_columns if col in df.columns]
    return df.select(column_order)


def is_selic_open() -> bool:
    """Verifica se o mercado está aberto no momento."""
    now = dt.datetime.now(TIMEZONE_BZ)
    today = now.date()
    time = now.time()
    is_last_bday = bday.is_business_day(today)
    is_trading_time = REALTIME_START_TIME <= time <= REALTIME_END_TIME

    return is_last_bday and is_trading_time


def tpf_intraday_trades() -> pd.DataFrame:
    """Fetches real-time secondary trading data for domestic Federal Public Debt
    (TPF - títulos públicos federais) from the Central Bank of Brazil (BCB).

    This function checks if the SELIC market is currently open based on Brazil/Sao_Paulo
    timezone business days and trading hours (defined by REALTIME_START_TIME and
    REALTIME_END_TIME). If the market is closed, or if no data is available from the
    source, or if an error occurs during fetching or processing, an empty DataFrame
    is returned. Otherwise, it retrieves, cleans, and processes the intraday trade
    data provided by BCB for Brazilian government bonds.

    Returns:
        pd.DataFrame: A DataFrame containing the latest intraday trades for FPD
            securities. Returns an empty DataFrame if the market is closed, no data
            is found, or an error occurs. The DataFrame includes the following columns:

            *   `SettlementDate`: The reference date for the spot market
                trading activity reported in this dataset (the current
                business day). Forward trades listed have future settlement dates
                not specified here.
            *   `BondType`: Abbreviation/ticker for the bond type (e.g., LFT,
                LTN, NTN-B).
            *   `SelicCode`: The unique SELIC code identifying the specific bond issue.
            *   `MaturityDate`: The maturity date of the bond.

            **Spot Market Data:**
            *   `MinPrice`: Minimum traded price.
            *   `AvgPrice`: Average traded price.
            *   `MaxPrice`: Maximum traded price.
            *   `LastPrice`: Last traded price.
            *   `MinRate`: Minimum traded yield/rate (as a decimal, e.g., 0.11 for 11%).
            *   `AvgRate`: Average traded yield/rate (as a decimal).
            *   `MaxRate`: Maximum traded yield/rate (as a decimal).
            *   `LastRate`: Last traded yield/rate (as a decimal).
            *   `Trades`: Total number of trades settled.
            *   `Quantity`: Total number of bonds traded (quantity).
            *   `Value`: Total financial value traded (in BRL).
            *   `BrokeredTrades`: Number of brokered trades settled.
            *   `BrokeredQuantity`: Quantity of bonds traded via brokers.

            **Forward Market Data:**
            *   `ForwardMinPrice`: Minimum traded price.
            *   `ForwardAvgPrice`: Average traded price.
            *   `ForwardMaxPrice`: Maximum traded price.
            *   `ForwardLastPrice`: Last traded price.
            *   `ForwardMinRate`: Minimum traded yield/rate (decimal).
            *   `ForwardAvgRate`: Average traded yield/rate (decimal).
            *   `ForwardMaxRate`: Maximum traded yield/rate (decimal).
            *   `ForwardLastRate`: Last traded yield/rate (decimal).
            *   `ForwardTrades`: Total number of trades contracted.
            *   `ForwardQuantity`: Total number of bonds traded (quantity).
            *   `ForwardValue`: Total financial value traded (in BRL).
            *   `ForwardBrokeredTrades`: Number of brokered trades contracted.
            *   `ForwardBrokeredQuantity`: Quantity of bonds traded via brokers.
    # Arrow note usage in data types
    Note:
        - The DataFrame returned by this function may be empty if the market is closed,
          no data is found, or an error occurs.
        - Arrow data types are used for better performance and compatibility with other
          libraries.
    """
    if not is_selic_open():
        logger.info("Market is closed. Returning empty DataFrame.")
        return pd.DataFrame()

    try:
        raw_text = _fetch_csv_from_url()
        cleaned_text = _clean_csv(raw_text)
        if not cleaned_text:
            logger.warning("No data found in the FPD intraday trades.")
            return pd.DataFrame()
        df = _convert_csv_to_df(cleaned_text)
        df = _process_df(df)
        df = _reorder_columns(df)
        value = df.select(pl.sum("Value")).item() / 10**9
        logger.info(f"Fetched {value:,.1f} billion BRL in FPD intraday trades.")
        return df.to_pandas(use_pyarrow_extension_array=True)
    except Exception as e:
        logger.exception(
            f"Error fetching data from BCB: {e}. Returning empty DataFrame."
        )
        return pd.DataFrame()
