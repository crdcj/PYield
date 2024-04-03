import pandas as pd
from pandas import Timestamp, DataFrame
from urllib.error import HTTPError

from . import di_futures as di
from . import br_calendar as cl

# URL Constants
ANBIMA_NON_MEMBER_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs/"
ANBIMA_MEMBER_URL = "http://www.anbima.associados.rtm/merc_sec/arqs/"

# Constant for conversion to basis points
BP_CONVERSION_FACTOR = 10_000


def normalize_date(reference_date: str | Timestamp | None = None) -> Timestamp:
    if isinstance(reference_date, str):
        normalized_date = pd.Timestamp(reference_date).normalize()
    elif isinstance(reference_date, Timestamp):
        normalized_date = reference_date.normalize()
    elif reference_date is None:
        today = pd.Timestamp.today().normalize()
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


def get_raw_data(
    reference_date: str | Timestamp | None = None, is_anbima_member: bool = False
) -> DataFrame:
    """
    Fetch indicative rates from ANBIMA for a specific date.

    Parameters:
    - reference_date (pd.Timestamp): Date for which to fetch the indicative rates.
    - is_anbima_member (bool): Whether the request is being made by an ANBIMA member.

    Returns:
    - pd.DataFrame: DataFrame with the indicative rates for the given date.
    """
    # Process the reference date, defaulting to the previous business day if not provided
    normalized_date = normalize_date(reference_date)

    # Format the date to match the URL format
    url_date = normalized_date.strftime("%y%m%d")

    # Set the base URL according to the member status
    base_url = ANBIMA_MEMBER_URL if is_anbima_member else ANBIMA_NON_MEMBER_URL
    # url example: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms231128.txt
    url = f"{base_url}ms{url_date}.txt"

    try:
        df = pd.read_csv(
            url,
            sep="@",
            encoding="latin-1",
            skiprows=2,
            decimal=",",
            thousands=".",
            dtype_backend="numpy_nullable",
        )
    except HTTPError:
        error_date = normalized_date.strftime("%d-%m-%Y")
        raise ValueError(f"Failed to get ANBIMA rates for {error_date}")

    return df


def process_raw_data(df_raw: DataFrame) -> DataFrame:
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
    is_anbima_member=False,
) -> DataFrame:
    """
    Fetch and process indicative rates from ANBIMA for a specific date.
    If no date is provided, the previous business day based on the Brazilian calendar
    is used.

     Parameters:
     - reference_date (str | pd.Timestamp | None): Date for which to fetch the indicative rates.
        If None, previous business day based on the Brazilian calendar is used.
     - return_raw (bool): Whether to return raw data without processing.
     - is_anbima_member (bool): Whether the request is being made by an ANBIMA member.

     Returns:
     - pd.DataFrame: DataFrame with the indicative rates for the given date.
    """

    normalized_date = normalize_date(reference_date)
    df = get_raw_data(normalized_date, is_anbima_member)

    if not return_raw:
        df = process_raw_data(df)

    return df


def calculate_treasury_di_spreads(
    reference_date: str | Timestamp | None = None,
    is_anbima_member=False,
) -> DataFrame:
    """
    Calculate the DI spread for LTN and NTN-F bonds based on ANBIMA's indicative rates.
    If no date is provided, the previous business day based on the Brazilian calendar
    is used.

    Parameters:
    - reference_date (str | pd.Timestamp | None): The reference date for querying ANBIMA's indicative rates.
        If None, the previous business day based on the Brazilian calendar is used.
    - is_anbima_member (bool): Specifies whether the request is made by an ANBIMA member.

    Returns:
    - pd.DataFrame: A DataFrame containing the bond type, reference date, maturity date, and DI spread in basis points.
    """
    # Validate the reference date, defaulting to the previous business day if not provided
    normalized_date = normalize_date(reference_date)

    # Fetch DI rates and adjust the maturity date format for compatibility
    df_di = di.get_di(normalized_date)[["ExpirationDate", "SettlementRate"]]

    # Renaming the columns to match the ANBIMA structure
    df_di.rename(columns={"ExpirationDate": "MaturityDate"}, inplace=True)

    # Adjusting maturity date to match bond data format
    df_di["MaturityDate"] = df_di["MaturityDate"].dt.to_period("M").dt.to_timestamp()

    # Fetch bond rates, filtering for LTN and NTN-F types
    df_anbima = get_treasury_rates(normalized_date, False, is_anbima_member)
    df_anbima.query("BondType in ['LTN', 'NTN-F']", inplace=True)

    # Merge bond and DI rates by maturity date to calculate spreads
    df_final = pd.merge(df_anbima, df_di, how="left", on="MaturityDate")

    # Calculating the DI spread as the difference between indicative and settlement rates
    df_final["DISpread"] = df_final["IndicativeRate"] - df_final["SettlementRate"]

    # Convert spread to basis points for clarity
    df_final["DISpread"] = (BP_CONVERSION_FACTOR * df_final["DISpread"]).round(2)

    # Prepare and return the final sorted DataFrame
    select_columns = ["BondType", "ReferenceDate", "MaturityDate", "DISpread"]
    df_final = df_final[select_columns].copy()
    return df_final.sort_values(["BondType", "MaturityDate"], ignore_index=True)
