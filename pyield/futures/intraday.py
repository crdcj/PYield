import pandas as pd
import requests


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
    df["TradeTimestamp"] = trade_ts

    # Convert DataFrame to use nullable data types for better type consistency
    df = df.convert_dtypes(dtype_backend="numpy_nullable")

    return df


def _process_raw_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()

    # Columns to be renamed
    all_columns = {
        "TradeTimestamp": "TradeTimestamp",
        "symb": "TickerSymbol",
        "mtrtyCode": "ExpirationDate",
        "BDaysToExp": "BDaysToExp",
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

    # df["BDaysToExp"] = bd.count_bdays(df["TradeTimestamp"], df["ExpirationDate"])

    # Remove percentage in all rate columns
    rate_cols = [col for col in df.columns if "Rate" in col]
    df[rate_cols] = df[rate_cols] / 100

    # Reorder columns based on the order of the dictionary
    return df[rename_dict.values()]


def fetch_intraday_df(future_code: str) -> pd.DataFrame:
    """
    Fetch the latest DI futures data from B3.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing the latest DI futures data.
    """
    raw_df = _fetch_raw_df(future_code)
    return _process_raw_df(raw_df)
