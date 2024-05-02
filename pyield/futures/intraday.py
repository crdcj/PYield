import pandas as pd
import requests

from .. import bday


def _fetch_raw_df(future_code: str) -> pd.DataFrame:
    """
    Fetch the latest data for a given future code from B3 derivatives quotation API.

    Args:
    future_code (str): The future code to fetch data for.

    Returns:
    pd.DataFrame: A DataFrame containing the normalized and cleaned data from the API.

    Raises:
    Exception: An exception is raised if the data fetch operation fails.
    """

    url = f"https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation/{future_code}"

    try:
        r = requests.get(url)
        r.raise_for_status()  # Check for HTTP request errors
    except requests.exceptions.RequestException:
        raise Exception(f"Failed to fetch data for {future_code}.") from None

    r.encoding = "utf-8"  # Explicitly set response encoding to utf-8 for consistency

    if "Quotation not available" in r.text:
        return pd.DataFrame()

    # Normalize JSON response into a flat table
    df = pd.json_normalize(r.json()["Scty"])

    # Clean and reformat the DataFrame columns
    df.columns = (df.columns
        .str.replace("SctyQtn.", "")
        .str.replace("asset.AsstSummry.", "")
    )  # fmt: skip
    df.drop(columns=["desc", "asset.code", "mkt.cd"], inplace=True)

    # Convert maturity codes to datetime and drop rows with missing values
    df["mtrtyCode"] = pd.to_datetime(df["mtrtyCode"], errors="coerce")
    df.dropna(subset=["mtrtyCode"], inplace=True)

    # Sort the DataFrame by maturity code and reset the index
    df.sort_values("mtrtyCode", inplace=True, ignore_index=True)

    # Get current date and time
    now = pd.Timestamp.now().round("s")
    # Subtract 15 minutes from the current time to account for API delay
    trade_ts = now - pd.Timedelta(minutes=15)
    df["TradeTime"] = trade_ts

    # Convert DataFrame to use nullable data types for better type consistency
    df = df.convert_dtypes(dtype_backend="numpy_nullable")

    return df


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename the columns of a DataFrame containing the futures data.

    Args:
    df (pd.DataFrame): A DataFrame containing futures data.

    Returns:
    pd.DataFrame: A DataFrame with the columns renamed.
    """
    all_columns = {
        "symb": "TickerSymbol",
        "mtrtyCode": "ExpirationDate",
        "opnCtrcts": "OpenContracts",
        "tradQty": "TradeCount",
        "traddCtrctsQty": "TradeVolume",
        "grssAmt": "FinancialVolume",
        "prvsDayAdjstmntPric": "PrevSettlementRate",
        "bottomLmtPric": "MinLimitRate",
        "topLmtPric": "MaxLimitRate",
        "opngPric": "OpenRate",
        "minPric": "MinRate",
        "avrgPric": "AvgRate",
        "maxPric": "MaxRate",
        "buyOffer.price": "CurrentAskRate",
        "sellOffer.price": "CurrentBidRate",
        "curPrc": "CurrentRate",
    }
    # Check which columns are present in the DataFrame before renaming
    rename_dict = {c: all_columns[c] for c in all_columns if c in df.columns}
    df = df.rename(columns=rename_dict)
    return df


def _process_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()

    df["BDaysToExp"] = bday.count_bdays(df["TradeTime"], df["ExpirationDate"])

    df["DaysToExp"] = (df["ExpirationDate"] - df["TradeTime"]).dt.days
    # Convert to nullable integer, since it is the default type in the library
    df["DaysToExp"] = df["DaysToExp"].astype(pd.Int64Dtype())

    # Remove percentage in all rate columns
    rate_cols = [col for col in df.columns if "Rate" in col]
    df[rate_cols] = df[rate_cols].div(100).round(5)

    return df


def _select_and_reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and reorder columns in the DataFrame.

    Args:
    df (pd.DataFrame): A DataFrame containing futures data.

    Returns:
    pd.DataFrame: A DataFrame with the columns selected and reordered.
    """
    columns = [
        "TradeTime",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        "OpenContracts",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "PrevSettlementRate",
        "MinLimitRate",
        "MaxLimitRate",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "CurrentAskRate",
        "CurrentBidRate",
        "CurrentRate",
    ]
    return df[columns]


def fetch_intraday_df(future_code: str) -> pd.DataFrame:
    """
    Fetch the latest futures data from B3.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing the latest DI futures data.
    """
    raw_df = _fetch_raw_df(future_code)
    if raw_df.empty:
        return raw_df
    df = _rename_columns(raw_df)
    df = _process_df(df)
    df = _select_and_reorder_columns(df)
    return df
