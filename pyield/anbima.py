import io

import pandas as pd
import requests

from pandas import Timestamp, DataFrame

from . import di
from . import calendar as cl

# URL Constants
ANBIMA_NON_MEMBER_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs/"
ANBIMA_MEMBER_URL = "http://www.anbima.associados.rtm/merc_sec/arqs/"

# Constant for conversion to basis points
BPS_CONVERSION_FACTOR = 10_000


def _normalize_date(reference_date: str | Timestamp | None = None) -> Timestamp:
    if isinstance(reference_date, str):
        normalized_date = pd.Timestamp(reference_date).normalize()
    elif isinstance(reference_date, Timestamp):
        normalized_date = reference_date.normalize()
    elif reference_date is None:
        today = pd.Timestamp.today().normalize()
        # Get last business day before today
        normalized_date = cl.offset_bdays(today, -1)
    else:
        raise ValueError("Invalid date format.")

    # Raise an error if the reference date is in the future
    if normalized_date > pd.Timestamp.today().normalize():
        raise ValueError("Reference date cannot be in the future.")

    # Raise error if the reference date is not a business day
    if not cl.is_business_day(normalized_date):
        raise ValueError("Reference date must be a business day.")

    return normalized_date


def _get_anbima_content(reference_date: Timestamp) -> str:
    url_date = reference_date.strftime("%y%m%d")
    member_url = f"{ANBIMA_MEMBER_URL}ms{url_date}.txt"
    non_member_url = f"{ANBIMA_NON_MEMBER_URL}ms{url_date}.txt"

    # Tries to access the member URL first
    try:
        response = requests.get(member_url)
        # Checks if the response was successful (status code 200)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException:
        # Blind attempt to access the member URL
        pass

    # If the member URL fails, tries to access the non-member URL
    try:
        response = requests.get(non_member_url)
        response.raise_for_status()  # Checks if the second attempt was successful
        return response.text
    except requests.exceptions.RequestException:
        # Both URLs failed
        return None


def _get_raw_data(reference_date: Timestamp) -> DataFrame:
    url_content = _get_anbima_content(reference_date)
    if url_content is None:
        date_str = reference_date.strftime("%d-%m-%Y")
        raise ValueError(f"Could not fetch ANBIMA data for {date_str}.")

    df = pd.read_csv(
        io.StringIO(url_content),
        sep="@",
        encoding="latin-1",
        skiprows=2,
        decimal=",",
        thousands=".",
        na_values=["--"],
        dtype_backend="numpy_nullable",
    )
    return df


def _process_raw_data(df_raw: DataFrame) -> DataFrame:
    """
    Process raw data from ANBIMA by filtering selected columns, renaming them, and adjusting data formats.

    Parameters:
    - df (pd.DataFrame): Raw data DataFrame to process.

    Returns:
    - pd.DataFrame: Processed DataFrame.
    """
    # Filter selected columns and rename them
    selected_columns_dict = {
        "Titulo": "BondType",
        "Data Referencia": "ReferenceDate",
        # "Codigo SELIC": "SelicCode",
        # "Data Base/Emissao": "IssueBaseDate",
        "Data Vencimento": "MaturityDate",
        "Tx. Compra": "BidRate",
        "Tx. Venda": "AskRate",
        "Tx. Indicativas": "IndicativeRate",
        "PU": "Price",
        # "Desvio padrao": "StdDev",
        # "Interv. Ind. Inf. (D0)",
        # "Interv. Ind. Sup. (D0)",
        # "Interv. Ind. Inf. (D+1)",
        # "Interv. Ind. Sup. (D+1)",
        # "Criterio": "Criteria",
    }
    select_columns = list(selected_columns_dict.keys())
    df = df_raw[select_columns].copy()
    df = df.rename(columns=selected_columns_dict)

    # Remove percentage from rates
    rate_cols = ["BidRate", "AskRate", "IndicativeRate"]
    df[rate_cols] = df[rate_cols] / 100

    df["ReferenceDate"] = pd.to_datetime(df["ReferenceDate"], format="%Y%m%d")
    df["MaturityDate"] = pd.to_datetime(df["MaturityDate"], format="%Y%m%d")

    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)


def get_treasury_rates(
    reference_date: str | Timestamp | None = None,
    return_raw=False,
) -> DataFrame:
    """
    Fetch and process indicative rates from ANBIMA for a specific date.
    If no date is provided, the previous business day based on the Brazilian calendar
    is used.

     Parameters:
     - reference_date (str | pd.Timestamp | None): Date for which to fetch the indicative rates.
        If None, the previous business day based on the Brazilian calendar is used.
     - return_raw (bool): Whether to return raw data without processing.

     Returns:
     - pd.DataFrame: DataFrame with the indicative rates for the given date.
    """

    normalized_date = _normalize_date(reference_date)
    df = _get_raw_data(normalized_date)

    if not return_raw:
        df = _process_raw_data(df)

    return df


def calculate_treasury_di_spreads(
    reference_date: str | Timestamp | None = None,
) -> DataFrame:
    """
    Calculate the DI spread for LTN and NTN-F bonds based on ANBIMA's indicative rates.
    If no date is provided, the previous business day based on the Brazilian calendar
    is used.

    Parameters:
    - reference_date (str | pd.Timestamp | None): The reference date for querying ANBIMA's indicative rates.
        If None, the previous business day based on the Brazilian calendar is used.

    Returns:
    - pd.DataFrame: A DataFrame containing the bond type, reference date, maturity date, and DI spread in basis points.
    """
    # Validate the reference date, defaulting to the previous business day if not provided
    normalized_date = _normalize_date(reference_date)

    # Fetch DI rates and adjust the maturity date format for compatibility
    df_di = di.get_di(normalized_date)[["ExpirationDate", "SettlementRate"]]

    # Renaming the columns to match the ANBIMA structure
    df_di.rename(columns={"ExpirationDate": "MaturityDate"}, inplace=True)

    # Adjusting maturity date to match bond data format
    df_di["MaturityDate"] = df_di["MaturityDate"].dt.to_period("M").dt.to_timestamp()

    # Fetch bond rates, filtering for LTN and NTN-F types
    df_anbima = get_treasury_rates(normalized_date, False)
    df_anbima.query("BondType in ['LTN', 'NTN-F']", inplace=True)

    # Merge bond and DI rates by maturity date to calculate spreads
    df_final = pd.merge(df_anbima, df_di, how="left", on="MaturityDate")

    # Calculating the DI spread as the difference between indicative and settlement rates
    df_final["DISpread"] = df_final["IndicativeRate"] - df_final["SettlementRate"]

    # Convert spread to basis points for clarity
    df_final["DISpread"] = (BPS_CONVERSION_FACTOR * df_final["DISpread"]).round(2)

    # Prepare and return the final sorted DataFrame
    select_columns = ["BondType", "ReferenceDate", "MaturityDate", "DISpread"]
    df_final = df_final[select_columns].copy()
    return df_final.sort_values(["BondType", "MaturityDate"], ignore_index=True)
