import io

import pandas as pd
import requests

from .. import bday


def get_expiration_date(expiration_code: str) -> pd.Timestamp:
    """
    Converts an expiration code into its corresponding expiration date.

    This function translates an expiration code into a specific expiration date based on
    a given mapping. The expiration code consists of a letter representing the month and
    two digits for the year. The function ensures the date returned is a valid business
    day by adjusting weekends and holidays as necessary.

    Args:
        expiration_code (str): The expiration code to be converted, where the first
            letter represents the month and the last two digits represent the year
            (e.g., "F23" for January 2023).

    Returns:
        pd.Timestamp: The expiration date corresponding to the code, adjusted to a valid
            business day. Returns pd.NaT if the code is invalid.

    Examples:
        >>> get_expiration_date("F23")
        pd.Timestamp('2023-01-01')

        >>> get_expiration_date("Z33")
        pd.Timestamp('2033-12-01')

        >>> get_expiration_date("A99")
        pd.NaT

    Notes:
        The expiration date is calculated based on the format change introduced by B3 on
        22-05-2006, where the first letter represents the month and the last two digits
        represent the year.
    """
    month_codes = {
        "F": 1,
        "G": 2,
        "H": 3,
        "J": 4,
        "K": 5,
        "M": 6,
        "N": 7,
        "Q": 8,
        "U": 9,
        "V": 10,
        "X": 11,
        "Z": 12,
    }

    try:
        month_code = expiration_code[0]
        month = month_codes[month_code]
        year = int("20" + expiration_code[-2:])
        # The expiration date is always the first business day of the month
        expiration = pd.Timestamp(year, month, 1)

        # Adjust to the next business day when expiration date is a weekend or a holiday
        adj_expiration = bday.offset_bdays(expiration, offset=0)

        return adj_expiration

    except (KeyError, ValueError):
        return pd.NaT  # type: ignore


def get_old_expiration_date(
    expiration_code: str, trade_date: pd.Timestamp
) -> pd.Timestamp:
    """
    Internal function to convert an old DI contract code into its ExpirationDate date.
    Valid for contract codes up to 21-05-2006.

    Args:
        expiration_code (str): An old DI Expiration Code from B3, where the first three
            letters represent the month and the last digit represents the year.
            Example: "JAN3".
        trade_date (pd.Timestamp): The trade date for which the contract code is valid.

    Returns:
        pd.Timestamp
            The contract's ExpirationDate date. Returns pd.NaT if the input is invalid.

    Examples:
        >>> get_old_expiration_date("JAN3", pd.Timestamp("2001-05-21"))
        pd.Timestamp('2003-01-01')

    Notes:
        - In 22-05-2006, B3 changed the format of the DI contract codes. Before that
        date, the first three letters represented the month and the last digit
        represented the year.
    """

    month_codes = {
        "JAN": 1,
        "FEV": 2,
        "MAR": 3,
        "ABR": 4,
        "MAI": 5,
        "JUN": 6,
        "JUL": 7,
        "AGO": 8,
        "SET": 9,
        "OUT": 10,
        "NOV": 11,
        "DEZ": 12,
    }
    try:
        month_code = expiration_code[:3]
        month = month_codes[month_code]

        # Year codes must generated dynamically, since it depends on the trade date.
        reference_year = trade_date.year
        year_codes = {}
        for year in range(reference_year, reference_year + 10):
            year_codes[str(year)[-1:]] = year
        year = year_codes[expiration_code[-1:]]

        expiration_date = pd.Timestamp(year, month, 1)
        # Adjust to the next business day when the date is a weekend or a holiday.
        # Must use old holiday list, since this contract code was used until 2006.
        return bday.offset_bdays(expiration_date, offset=0, holiday_list="old")

    except (KeyError, ValueError):
        return pd.NaT  # type: ignore


def fetch_past_raw_df(asset_code: str, trade_date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the historical futures data from B3 for a specific trade date.

    Args:
        trade_date (pd.Timestamp): The trade date for which the data should be fetched.

    Returns:
        pd.DataFrame: Raw DI data as a Pandas pd.DataFrame.
    """
    url_date = trade_date.strftime("%d/%m/%Y")
    # url example: https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data=05/10/2023&Mercadoria=DI1
    url = f"https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data={url_date}&Mercadoria={asset_code}&XLS=false"
    r = requests.get(url)

    text = r.text
    if "AJUSTE" not in text:
        raise ValueError(f"Could not fetch data for {url_date}.")

    df = pd.read_html(
        io.StringIO(text),
        match="AJUSTE",
        header=1,
        thousands=".",
        decimal=",",
        na_values=["-"],
        dtype_backend="numpy_nullable",
    )[0]

    # Remove rows with all NaN values
    df = df.dropna(how="all")

    # Remove columns with all NaN values
    df = df.dropna(axis=1, how="all")

    # Force "VAR. PTOS." to be string, since it can also be read as float
    df["VAR. PTOS."] = df["VAR. PTOS."].astype(pd.StringDtype())

    # Force "AJUSTE CORRIG. (4)" to be float, since it can be also read as int
    if "AJUSTE CORRIG. (4)" in df.columns:
        df["AJUSTE CORRIG. (4)"] = df["AJUSTE CORRIG. (4)"].astype(pd.Float64Dtype())

    return df


def fetch_last_raw_df(future_code: str) -> pd.DataFrame:
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
