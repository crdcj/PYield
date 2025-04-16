import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pyield import bday
from pyield.fwd import forwards
from pyield.retry import default_retry

BASE_URL = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation"

# Timezone for Brazil
BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")

# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
INTRADAY_START_TIME = dt.time(9, 16)

# Set up logging
logger = logging.getLogger(__name__)


@default_retry
def _fetch_b3_df(contract_code: str) -> pd.DataFrame:
    """
    Fetch the latest data for a given future code from B3 derivatives quotation API.

    Args:
        future_code (str): The future code to fetch data for.

    Returns:
        pd.DataFrame: DataFrame containing the normalized and cleaned data from the API.
            If no data is available, an empty DataFrame is returned.

    Raises:
        Exception: An exception is raised if the data fetch operation fails.
    """

    url = f"{BASE_URL}/{contract_code}"

    r = requests.get(url, timeout=10)
    r.raise_for_status()  # Check for HTTP request errors
    r.encoding = "utf-8"  # Explicitly set response encoding to utf-8 for consistency

    # Check if the response contains the expected data
    if "Quotation not available" in r.text or "curPrc" not in r.text:
        return pd.DataFrame()

    # Normalize JSON response into a flat table
    df = pd.json_normalize(r.json()["Scty"])

    # Convert columns to the most appropriate data type
    return df.convert_dtypes()


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename the columns of a DataFrame containing the futures data.

    Args:
        df (pd.DataFrame): A DataFrame containing futures data.

    Returns:
        pd.DataFrame: DataFrame with the columns renamed.
    """
    all_columns = {
        "symb": "TickerSymbol",
        "bottomLmtPric": "MinLimitRate",
        "prvsDayAdjstmntPric": "PrevSettlementRate",
        "topLmtPric": "MaxLimitRate",
        "opngPric": "OpenRate",
        "minPric": "MinRate",
        "maxPric": "MaxRate",
        "avrgPric": "AvgRate",
        "curPrc": "LastRate",
        "grssAmt": "FinancialVolume",
        "mtrtyCode": "ExpirationDate",
        "opnCtrcts": "OpenContracts",
        "tradQty": "TradeCount",
        "traddCtrctsQty": "TradeVolume",
        "buyOffer.price": "LastAskRate",
        "sellOffer.price": "LastBidRate",
    }
    # Check which columns are present in the DataFrame before renaming
    rename_dict = {c: all_columns[c] for c in all_columns if c in df.columns}
    df = df.rename(columns=rename_dict)
    return df


def _process_df(raw_df: pd.DataFrame, contract_code: str) -> pd.DataFrame:
    """Process the raw DataFrame from B3 API."""
    df = raw_df.copy()

    # Clean and reformat the DataFrame columns
    df.columns = (df.columns
        .str.replace("SctyQtn.", "")
        .str.replace("asset.AsstSummry.", "")
    )  # fmt: skip
    df.drop(columns=["desc", "asset.code", "mkt.cd"], inplace=True)

    df = _rename_columns(df)

    # Convert maturity codes to datetime and drop rows with missing values
    df["ExpirationDate"] = pd.to_datetime(df["ExpirationDate"], errors="coerce")
    df.dropna(subset=["ExpirationDate"], inplace=True)

    # Sort the DataFrame by maturity code and reset the index
    df.sort_values("ExpirationDate", inplace=True, ignore_index=True)

    # Get currante date in Brazil
    df["TradeDate"] = bday.last_business_day()

    # Get current date and time
    now = pd.Timestamp.now(BZ_TIMEZONE).round("s").tz_localize(None)
    # Subtract 15 minutes from the current time to account for API delay
    df["LastUpdate"] = now - pd.Timedelta(minutes=15)

    df["BDaysToExp"] = bday.count(df["TradeDate"], df["ExpirationDate"])

    df["DaysToExp"] = (df["ExpirationDate"] - df["TradeDate"]).dt.days
    # Convert to nullable integer, since it is the default type in the library
    df["DaysToExp"] = df["DaysToExp"].astype("Int64")

    # Remove expired contracts
    df = df.query("DaysToExp >= 0").reset_index(drop=True)

    # The "FinancialVolume" column is in BRL, so we need to round it to cents
    df["FinancialVolume"] = df["FinancialVolume"].round(2)

    # Remove percentage in all rate columns
    rate_cols = [col for col in df.columns if "Rate" in col]
    df[rate_cols] = df[rate_cols].div(100).round(5)

    if contract_code in {"DI1", "DAP"}:  # Add LastPrice for DI1 and DAP
        byears = df["BDaysToExp"] / 252
        df["LastPrice"] = 100_000 / ((1 + df["LastRate"]) ** byears)
        df["LastPrice"] = df["LastPrice"].round(2).astype("Float64")
        df["ForwardRate"] = forwards(bdays=df["BDaysToExp"], rates=df["LastRate"])

    if contract_code == "DI1":  # Add DV01 for DI1
        duration = df["BDaysToExp"] / 252
        modified_duration = duration / (1 + df["LastRate"])
        df["DV01"] = 0.0001 * modified_duration * df["LastPrice"]

    return df


def _select_and_reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and reorder columns in the DataFrame.

    Args:
        df (pd.DataFrame): A DataFrame containing futures data.

    Returns:
        pd.DataFrame: DataFrame with the columns selected and reordered.
    """
    all_columns = [
        "TradeDate",
        "LastUpdate",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        "OpenContracts",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "DV01",
        "LastPrice",
        "PrevSettlementRate",
        "MinLimitRate",
        "MaxLimitRate",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "LastAskRate",
        "LastBidRate",
        "LastRate",
        "ForwardRate",
    ]
    reordered_columns = [col for col in all_columns if col in df.columns]
    return df[reordered_columns].copy()


def fetch_intraday_df(contract_code: str) -> pd.DataFrame:
    """
    Fetch the latest futures data from B3.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing the latest DI futures data.
    """
    raw_df = _fetch_b3_df(contract_code)
    if raw_df.empty:
        date_str = dt.datetime.now(BZ_TIMEZONE).strftime("%d-%m-%Y %H:%M")
        logger.warning(
            f"No data available for {contract_code} on {date_str}. "
            f"Returning an empty DataFrame."
        )

        return pd.DataFrame()
    df = _process_df(raw_df, contract_code)
    df = _select_and_reorder_columns(df)
    return df
