import datetime as dt
import logging
from typing import Literal
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

import pandas as pd

from pyield import bday
from pyield.data_cache import get_cached_dataset
from pyield.date_converter import DateScalar, convert_input_dates
from pyield.retry import default_retry

BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")

BOND_TYPES = Literal["LTN", "NTN-B", "NTN-C", "NTN-F", "LFT"]

ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs"
ANBIMA_RTM_URL = "http://www.anbima.associados.rtm/merc_sec/arqs"
# URL example: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt

# Before 13/05/2014 the file was zipped and the endpoint ended with ".exe"
FORMAT_CHANGE_DATE = pd.to_datetime("13-05-2014", dayfirst=True)

logger = logging.getLogger(__name__)


def _bond_type_mapping(bond_type: str) -> str:
    bond_type = bond_type.upper()
    bond_type_mapping = {"NTNB": "NTN-B", "NTNC": "NTN-C", "NTNF": "NTN-F"}
    return bond_type_mapping.get(bond_type, bond_type)


def _build_file_name(date: pd.Timestamp) -> str:
    url_date = date.strftime("%y%m%d")
    if date < FORMAT_CHANGE_DATE:
        file_name = f"ms{url_date}.exe"
    else:
        file_name = f"ms{url_date}.txt"
    return file_name


def _build_file_url(date: pd.Timestamp) -> str:
    today = dt.datetime.now(BZ_TIMEZONE).date()
    business_days_count = bday.count(date, today)
    if business_days_count > 5:  # noqa
        # For dates older than 5 business days, only the RTM data is available
        logger.info(f"Trying to fetch RTM data for {date.strftime('%d/%m/%Y')}")
        file_url = f"{ANBIMA_RTM_URL}/{_build_file_name(date)}"
    else:
        file_url = f"{ANBIMA_URL}/{_build_file_name(date)}"
    return file_url


@default_retry
def _read_raw_df(file_url: str) -> pd.DataFrame:
    df = pd.read_csv(
        file_url,
        sep="@",
        encoding="latin1",
        skiprows=2,
        decimal=",",
        thousands=".",
        na_values=["--"],
        dtype_backend="numpy_nullable",
    )
    return df


def _process_raw_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    # Filter selected columns and rename them
    selected_columns_dict = {
        "Titulo": "BondType",
        "Data Referencia": "ReferenceDate",
        "Codigo SELIC": "SelicCode",
        "Data Base/Emissao": "IssueBaseDate",
        "Data Vencimento": "MaturityDate",
        "Tx. Compra": "BidRate",
        "Tx. Venda": "AskRate",
        "Tx. Indicativas": "IndicativeRate",
        "PU": "Price",
        "Desvio padrao": "StdDev",
        "Interv. Ind. Inf. (D0)": "LowerBoundRateD0",
        "Interv. Ind. Sup. (D0)": "UpperBoundRateD0",
        "Interv. Ind. Inf. (D+1)": "LowerBoundRateD1",
        "Interv. Ind. Sup. (D+1)": "UpperBoundRateD1",
        "Criterio": "Criteria",
    }
    df = df_raw.rename(columns=selected_columns_dict)

    # Remove percentage from rates
    rate_cols = [col for col in df.columns if "Rate" in col]
    # Rate columns have percentage values with 4 decimal places in percentage values
    # We will round to 6 decimal places to avoid floating point errors
    df[rate_cols] = (df[rate_cols] / 100).round(6)

    df["ReferenceDate"] = pd.to_datetime(df["ReferenceDate"], format="%Y%m%d")
    df["MaturityDate"] = pd.to_datetime(df["MaturityDate"], format="%Y%m%d")
    df["IssueBaseDate"] = pd.to_datetime(df["IssueBaseDate"], format="%Y%m%d")

    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)


def tpf_web_data(
    date: DateScalar, bond_type: str | BOND_TYPES | None = None
) -> pd.DataFrame:
    """Fetch and process TPF secondary market data directly from the ANBIMA website.
    Only the last 5 days of data are available in the ANBIMA website.

    This function retrieves bond market data from the ANBIMA website for a
    specified reference date. It handles different file formats based on the date
    and attempts to download the data from both member and non-member URLs.

    Args:
        date (DateScalar): The reference date for the data.
        bond_type (str, optional):  Filter data by bond type.
            Defaults to None, which returns data for all bond types.

    Returns:
        pd.DataFrame: A DataFrame containing bond market data.
            Returns an empty DataFrame if data is not available for the
            specified date (weekends, holidays, or unavailable data).
    """
    date = convert_input_dates(date)
    date_log = date.strftime("%d/%m/%Y")

    try:
        file_url = _build_file_url(date)
        df = _read_raw_df(file_url)
        if df.empty:
            logger.info(
                f"Anbima TPF secondary market data for {date_log} not available."
                "Returning empty DataFrame."
            )
            return df
        df = _process_raw_df(df)
        if bond_type:
            norm_bond_type = _bond_type_mapping(bond_type)  # noqa
            df = df.query("BondType == @norm_bond_type").reset_index(drop=True)
        return df.sort_values(["BondType", "MaturityDate"]).reset_index(drop=True)
    except HTTPError as e:
        if e.code == 404:  # noqa
            logger.info(
                f"No Anbima TPF secondary market data for {date_log}. "
                "Returning empty DataFrame."
            )
            return pd.DataFrame()
        raise  # Propagate other HTTP errors
    except Exception:
        logger.exception(f"Error fetching TPF data for {date_log}")
        raise


def tpf_data(
    date: DateScalar,
    bond_type: str | None = None,
    adj_maturities: bool = False,
) -> pd.DataFrame:
    """Retrieve indicative rates for bonds from ANBIMA data.

    This function fetches indicative rates for bonds from ANBIMA,
    initially attempting to retrieve data from a cached dataset. If the data
    is not available in the cache, it fetches it directly from the ANBIMA website.

    Args:
        date (DateScalar): The reference date for the rates.
        bond_type (str, optional): Filter rates by bond type.
            Defaults to None, which returns rates for all bond types.
        adj_maturities (bool, optional): Adjust maturity dates to the next
            business day. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with the following columns:
            - BondType: The type of bond.
            - MaturityDate: The maturity date of the bond.
            - IndicativeRate: The indicative rate of the bond.
            - Price: The price (PU) of the bond.
    """

    df = get_cached_dataset("tpf")
    date = convert_input_dates(date)
    df = df.query("ReferenceDate == @date").reset_index(drop=True)

    if df.empty:
        # Try to fetch the data from the Anbima website
        df = tpf_web_data(date)

    if df.empty:
        # If the data is still empty, return an empty DataFrame
        return pd.DataFrame()

    if bond_type:
        df = df.query("BondType == @bond_type").reset_index(drop=True)

    if adj_maturities:
        df["MaturityDate"] = bday.offset(df["MaturityDate"], 0)

    return (
        df[["ReferenceDate", "BondType", "MaturityDate", "IndicativeRate", "Price"]]
        .sort_values(["BondType", "MaturityDate"])
        .reset_index(drop=True)
    )


def tpf_fixed_rate_maturities(date: DateScalar) -> pd.Series:
    """Retrieve pre-defined maturity dates for LTN and NTN-F bonds.

    This function fetches pre-defined maturity dates for 'LTN' (prefixadas) and
    'NTN-F' (indexadas ao CDI) bond types from the cached ANBIMA dataset
    for a given reference date.

    Args:
        date (DateScalar): The reference date for maturity dates.

    Returns:
        pd.Series: A Series containing unique maturity dates for 'LTN' and
            'NTN-F' bonds, sorted in ascending order.
    """
    maturity_dates = (
        tpf_data(date)
        .query("BondType in ['LTN', 'NTN-F']")["MaturityDate"]
        .drop_duplicates()
        .sort_values(ignore_index=True)
        .reset_index(drop=True)
    )
    return maturity_dates
