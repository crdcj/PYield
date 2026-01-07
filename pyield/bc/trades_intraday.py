"""
Fetches real-time secondary trading data for domestic Federal Public Debt (FPD)
https://www.bcb.gov.br/htms/selic/selicprecos.asp?frame=1
"""

import datetime as dt
import io
import logging
from pathlib import Path

import polars as pl
import requests
from polars import selectors as cs

from pyield import bday, clock

REALTIME_START_TIME = dt.time(9, 0, 0)
REALTIME_END_TIME = dt.time(22, 0, 0)

logger = logging.getLogger(__name__)
test_file = Path(__file__).parent.parent.parent / "tests/data/negocios-registrados.csv"

API_COL_MAPPING = {
    "//1": "RowType",
    "código título": "SelicCode",
    "data vencimento": "MaturityDate",
    "sigla": "BondType",
    "mercado à vista pu último": "LastPrice",
    "tx último": "LastRate",
    "pu mínimo": "MinPrice",
    "tx mínimo": "MinRate",
    "pu médio": "AvgPrice",
    "tx médio": "AvgRate",
    "pu máximo": "MaxPrice",
    "tx máximo": "MaxRate",
    "totais liquidados operações": "Trades",
    "corretagem liquidados operações": "BrokeredTrades",
    "títulos": "Quantity",
    "corretagem títulos": "BrokeredQuantity",
    "financeiro": "Value",
    "mercado a termo pu último": "FwdLastPrice",
    "tx último_duplicated_0": "FwdLastRate",
    "pu mínimo_duplicated_0": "FwdMinPrice",
    "tx mínimo_duplicated_0": "FwdMinRate",
    "pu médio_duplicated_0": "FwdAvgPrice",
    "tx médio_duplicated_0": "FwdAvgRate",
    "pu máximo_duplicated_0": "FwdMaxPrice",
    "tx máximo_duplicated_0": "FwdMaxRate",
    "totais contratados operações": "FwdTrades",
    "corretagem contratados operações": "FwdBrokeredTrades",
    "títulos_duplicated_0": "FwdQuantity",
    "corretagem títulos_duplicated_0": "FwdBrokeredQuantity",
    "financeiro_duplicated_0": "FwdValue",
}

DATA_SCHEMA = {
    "SelicCode": pl.Int64,
    "LastPrice": pl.Float64,
    "LastRate": pl.Float64,
    "MinPrice": pl.Float64,
    "MinRate": pl.Float64,
    "AvgPrice": pl.Float64,
    "AvgRate": pl.Float64,
    "MaxPrice": pl.Float64,
    "MaxRate": pl.Float64,
    "Trades": pl.Int64,
    "BrokeredTrades": pl.Int64,
    "Quantity": pl.Int64,
    "BrokeredQuantity": pl.Int64,
    "Value": pl.Float64,
    "FwdLastPrice": pl.Float64,
    "FwdLastRate": pl.Float64,
    "FwdMinPrice": pl.Float64,
    "FwdMinRate": pl.Float64,
    "FwdAvgPrice": pl.Float64,
    "FwdAvgRate": pl.Float64,
    "FwdMaxPrice": pl.Float64,
    "FwdMaxRate": pl.Float64,
    "FwdTrades": pl.Int64,
    "FwdBrokeredTrades": pl.Int64,
    "FwdQuantity": pl.Int64,
    "FwdBrokeredQuantity": pl.Int64,
    "FwdValue": pl.Float64,
}

FINAL_COLUMN_ORDER = [
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
    "FwdMinPrice",
    "FwdAvgPrice",
    "FwdLastPrice",
    "FwdMaxPrice",
    "FwdLastRate",
    "FwdMinRate",
    "FwdAvgRate",
    "FwdMaxRate",
    "FwdTrades",
    "FwdQuantity",
    "FwdValue",
    "FwdBrokeredTrades",
    "FwdBrokeredQuantity",
]


def _fetch_csv_from_url() -> str:
    """
    Example URL for the CSV file containing intraday trades data:
        https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/02-06-2025
    """
    today = clock.today()
    formatted_date = today.strftime("%d-%m-%Y")
    FILE_URL = f"https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/{formatted_date}"
    r = requests.get(FILE_URL, timeout=30)  # API usually takes 10s to respond
    r.raise_for_status()
    r.encoding = "utf-8-sig"  # Handle BOM for UTF-8
    return r.text


def _clean_csv(text: str) -> str:
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
    now = clock.now()
    today = now.date()

    # 1. Strip column names from source
    df.columns = [col.strip() for col in df.columns]

    # 2. Main processing pipeline
    df = (
        df.rename(API_COL_MAPPING)
        .cast(DATA_SCHEMA, strict=False)
        .drop(["RowType"], strict=False)
        .with_columns(
            pl.col("BondType").str.strip_chars(),
            pl.col("MaturityDate").str.strptime(pl.Date, "%d/%m/%Y"),
            pl.lit(today).alias("SettlementDate"),
            pl.lit(now).alias("CollectedAt"),
            (cs.contains("Rate") / 100).round(6),
        )
    )

    # 3. Final selection and reordering
    final_columns = [col for col in FINAL_COLUMN_ORDER if col in df.columns]

    return df.select(final_columns)


def is_selic_open() -> bool:
    """Verifica se o mercado está aberto no momento."""
    now = clock.now()
    today = now.date()
    time = now.time()
    is_last_bday = bday.is_business_day(today)
    is_trading_time = REALTIME_START_TIME <= time <= REALTIME_END_TIME

    return is_last_bday and is_trading_time


def tpf_intraday_trades() -> pl.DataFrame:
    """Fetches real-time secondary trading data for domestic Federal Public Debt
    (TPF - títulos públicos federais) from the Central Bank of Brazil (BCB).

    This function checks if the SELIC market is currently open based on Brazil/Sao_Paulo
    timezone business days and trading hours (defined by REALTIME_START_TIME and
    REALTIME_END_TIME). If the market is closed, or if no data is available from the
    source, or if an error occurs during fetching or processing, an empty DataFrame
    is returned. Otherwise, it retrieves, cleans, and processes the intraday trade
    data provided by BCB for Brazilian government bonds.

    Returns:
        pl.DataFrame: A DataFrame containing the latest intraday trades for FPD
            securities. Returns an empty DataFrame if the market is closed, no data
            is found, or an error occurs. The DataFrame includes the following columns:

    DataFrame Columns:
        - `CollectedAt`: Timestamp indicating when the data was collected
            (in Brazil/Sao_Paulo timezone).
        - `SettlementDate`: The reference date for the spot market
            trading activity reported in this dataset (the current
            business day). Forward trades listed have future settlement dates
            not specified here.
        - `BondType`: Abbreviation/ticker for the bond type (e.g., LFT,
            LTN, NTN-B).
        - `SelicCode`: The unique SELIC code identifying the specific bond issue.
        - `MaturityDate`: The maturity date of the bond.
        - `MinPrice`: Minimum traded price.
        - `AvgPrice`: Average traded price.
        - `MaxPrice`: Maximum traded price.
        - `LastPrice`: Last traded price.
        - `MinRate`: Minimum traded yield/rate (as a decimal, e.g., 0.11 for 11%).
        - `AvgRate`: Average traded yield/rate (as a decimal).
        - `MaxRate`: Maximum traded yield/rate (as a decimal).
        - `LastRate`: Last traded yield/rate (as a decimal).
        - `Trades`: Total number of trades settled.
        - `Quantity`: Total number of bonds traded (quantity).
        - `Value`: Total financial value traded (in BRL).
        - `BrokeredTrades`: Number of brokered trades settled.
        - `BrokeredQuantity`: Quantity of bonds traded via brokers.
        - `FwdMinPrice`: Forward minimum traded price.
        - `FwdAvgPrice`: Forward average traded price.
        - `FwdMaxPrice`: Forward maximum traded price.
        - `FwdLastPrice`: Forward last traded price.
        - `FwdMinRate`: Forward minimum traded yield/rate (decimal).
        - `FwdAvgRate`: Forward average traded yield/rate (decimal).
        - `FwdMaxRate`: Forward maximum traded yield/rate (decimal).
        - `FwdLastRate`: Forward last traded yield/rate (decimal).
        - `FwdTrades`: Forward total number of trades contracted.
        - `FwdQuantity`: Forward total number of bonds traded (quantity).
        - `FwdValue`: Forward total financial value traded (in BRL).
        - `FwdBrokeredTrades`: Forward number of brokered trades contracted.
        - `FwdBrokeredQuantity`: Forward quantity of bonds traded via brokers.

    Notes:
        - The DataFrame returned by this function may be empty if the market is closed,
          no data is found, or an error occurs.
        - Arrow data types are used for better performance and compatibility with other
          libraries.
    """
    if not is_selic_open():
        logger.info("Market is closed. Returning empty DataFrame.")
        return pl.DataFrame()

    try:
        raw_text = _fetch_csv_from_url()
        cleaned_text = _clean_csv(raw_text)
        if not cleaned_text:
            logger.warning("No data found in the FPD intraday trades.")
            return pl.DataFrame()

        df = _convert_csv_to_df(cleaned_text)
        df = _process_df(df)

        value = df["Value"].sum() / 10**9
        logger.info(f"Fetched {value:,.1f} billion BRL in FPD intraday trades.")
        return df
    except Exception as e:
        logger.exception(
            f"Error fetching data from BCB: {e}. Returning empty DataFrame."
        )
        return pl.DataFrame()
