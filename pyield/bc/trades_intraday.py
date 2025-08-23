"""
Fetches real-time secondary trading data for domestic Federal Public Debt (FPD)
https://www.bcb.gov.br/htms/selic/selicprecos.asp?frame=1
"""

import datetime as dt
import io
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pyield import bday

BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")
REALTIME_START_TIME = dt.time(9, 0, 0)
REALTIME_END_TIME = dt.time(22, 0, 0)

logger = logging.getLogger(__name__)
test_file = Path(__file__).parent.parent.parent / "tests/data/negocios-registrados.csv"


def _fetch_csv_from_url() -> str:
    """
    Example URL for the CSV file containing intraday trades data:
        https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/02-06-2025
    """
    today = dt.datetime.now(BZ_TIMEZONE).date()
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
    return "\n".join(valid_rows)


def _convert_csv_to_df(text: str) -> pd.DataFrame:
    return pd.read_csv(
        io.StringIO(text),
        sep=";",
        decimal=",",
        thousands=".",
        dtype_backend="pyarrow",
        na_values="-",
    )


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
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
        " financeiro": "Volume",
        " mercado a termo pu último": "ForwardLastPrice",
        " tx último.1": "ForwardLastRate",
        " pu mínimo.1": "ForwardMinPrice",
        " tx mínimo.1": "ForwardMinRate",
        " pu médio.1": "ForwardAvgPrice",
        " tx médio.1": "ForwardAvgRate",
        " pu máximo.1": "ForwardMaxPrice",
        " tx máximo.1": "ForwardMaxRate",
        " totais contratados operações": "ForwardTrades",
        " corretagem contratados operações": "ForwardBrokeredTrades",
        " títulos.1": "ForwardQuantity",
        " corretagem títulos.1": "ForwardBrokeredQuantity",
        " financeiro.1": "ForwardVolume",
    }

    df = df.rename(columns=col_mapping)
    df = df.drop(columns=["RowType"], errors="ignore")
    df["BondType"] = df["BondType"].str.strip()
    df["MaturityDate"] = pd.to_datetime(df["MaturityDate"], format="%d/%m/%Y")
    now = dt.datetime.now(BZ_TIMEZONE)
    df["SettlementDate"] = pd.Timestamp(now.date())
    df["CollectedAt"] = pd.Timestamp(now)
    df["CollectedAt"] = df["CollectedAt"].astype("timestamp[ns][pyarrow]")

    for col in [c for c in df.columns if c.endswith("Date")]:
        df[col] = df[col].astype("date32[pyarrow]")

    # Remove percentage from rate columns
    rate_cols = [col for col in df.columns if "Rate" in col]
    for col in rate_cols:
        df[col] = (df[col] / 100).round(6)

    return df


def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
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
        "Volume",
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
        "ForwardVolume",
        "ForwardBrokeredTrades",
        "ForwardBrokeredQuantity",
    ]

    column_order = [col for col in reorder_columns if col in df.columns]
    return df[column_order].copy()


def is_selic_open() -> bool:
    """Verifica se o mercado está aberto no momento."""
    now = dt.datetime.now(BZ_TIMEZONE)
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
            *   `Volume`: Total financial volume traded (in BRL).
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
            *   `ForwardVolume`: Total financial volume traded (in BRL).
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
        # raw_text = test_file.read_text(encoding="iso-8859-15")
        cleaned_text = _clean_csv(raw_text)
        if not cleaned_text:
            logger.warning("No data found in the FPD intraday trades.")
            return pd.DataFrame()
        df = _convert_csv_to_df(cleaned_text)
        df = _process_df(df)
        df = _reorder_columns(df)
        volume = df["Volume"].sum() / 10**9
        logger.info(f"Fetched {volume:,.1f} billion BRL in FPD intraday trades.")
        return df
    except Exception:
        logger.exception("Error fetching data from BCB. Returning empty DataFrame.")
        return pd.DataFrame()
