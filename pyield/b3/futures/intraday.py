from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pyield import bday
from pyield.retry import default_retry

# Timezone for Brazil
TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")
BASE_URL = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation"


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


def _process_df(raw_df: pd.DataFrame) -> pd.DataFrame:
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
    now = pd.Timestamp.now(TIMEZONE_BZ).round("s").tz_localize(None)
    # Subtract 15 minutes from the current time to account for API delay
    df["LastUpdate"] = now - pd.Timedelta(minutes=15)

    df["BDaysToExp"] = bday.count(df["TradeDate"], df["ExpirationDate"])

    df["DaysToExp"] = (df["ExpirationDate"] - df["TradeDate"]).dt.days
    # Convert to nullable integer, since it is the default type in the library
    df["DaysToExp"] = df["DaysToExp"].astype("Int64")

    # The "FinancialVolume" column is in BRL, so we need to round it to cents
    df["FinancialVolume"] = df["FinancialVolume"].round(2)

    # Remove percentage in all rate columns
    rate_cols = [col for col in df.columns if "Rate" in col]
    df[rate_cols] = df[rate_cols].div(100).round(5)

    # Adjust DI1 futures contracts
    contract_type = df["TickerSymbol"].iloc[0][:3]
    if contract_type in {"DI1", "DAP"}:
        byears = df["BDaysToExp"] / 252
        df["LastPrice"] = 100_000 / ((1 + df["LastRate"]) ** (byears))
        df["LastPrice"] = df["LastPrice"].round(2).astype("Float64")

    if contract_type == "DI1":
        duration = df["BDaysToExp"] / 252
        modified_duration = duration / (1 + df["LastRate"])
        df["DV01"] = 0.0001 * modified_duration * df["LastPrice"]
        df["DV01"] = df["DV01"].astype("Float64")

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
        "LastPrice",
    ]
    reordered_columns = [col for col in all_columns if col in df.columns]
    return df[reordered_columns].copy()


def fetch_intraday_df(future_code: str) -> pd.DataFrame:
    """
    Fetch the latest futures data from B3.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing the latest DI futures data.
    """
    raw_df = _fetch_b3_df(future_code)
    if raw_df.empty:
        return pd.DataFrame()
    df = _process_df(raw_df)
    df = _select_and_reorder_columns(df)
    return df
