import logging
from typing import Literal
from urllib.error import HTTPError

import pandas as pd

from pyield import bday
from pyield.data_cache import get_anbima_dataset
from pyield.date_converter import DateScalar, convert_input_dates
from pyield.retry import default_retry

BOND_TYPES = Literal["LTN", "NTN-B", "NTN-C", "NTN-F", "LFT"]
ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs"
# URL example: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt

logger = logging.getLogger(__name__)


def _bond_type_mapping(bond_type: str) -> str:
    bond_type = bond_type.upper()
    bond_type_mapping = {"NTNB": "NTN-B", "NTNC": "NTN-C", "NTNF": "NTN-F"}
    return bond_type_mapping.get(bond_type, bond_type)


def _build_file_url(date: pd.Timestamp) -> str:
    url_date = date.strftime("%y%m%d")
    filename = f"ms{url_date}.txt"
    file_url = f"{ANBIMA_URL}/{filename}"
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


def tpf_data(
    date: DateScalar, bond_type: str | BOND_TYPES | None = None
) -> pd.DataFrame:
    """Fetch and process TPF secondary market data from ANBIMA.

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
                f"No TPF secondary market data from ANBIMA for {date_log}. "
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
                f"No TPF secondary market data from ANBIMA for {date_log}. "
                "Returning empty DataFrame."
            )
            return pd.DataFrame()
        raise  # Propagate other HTTP errors
    except Exception:
        logger.exception(f"Error fetching TPF data for {date_log}")
        raise


def tpf_rates(
    date: DateScalar,
    bond_type: str | None = None,
    adj_maturities: bool = False,
) -> pd.DataFrame:
    """Retrieve indicative rates for bonds from ANBIMA data.

    This function fetches indicative interest rates for bonds from ANBIMA,
    initially attempting to retrieve data from a cached dataset. If the data
    is not available in the cache, it fetches it directly from the ANBIMA
    website using the :func:`data` function.

    Args:
        date (DateScalar): The reference date for the rates.
        bond_type (str, optional): Filter rates by bond type.
            Defaults to None, which returns rates for all bond types.
        adj_maturities (bool, optional): Adjust maturity dates to the next
            business day. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing bond types, maturity dates, and
            indicative rates. Returns an empty DataFrame if data is not
            available for the specified date.
    """
    df = get_anbima_dataset()
    date = convert_input_dates(date)
    df = df.query("ReferenceDate == @date").reset_index(drop=True)

    if df.empty:
        # Try to fetch the data from the Anbima website
        df = tpf_data(date)

    if df.empty:
        # If the data is still empty, return an empty DataFrame
        return pd.DataFrame()

    if bond_type:
        df = df.query("BondType == @bond_type").reset_index(drop=True)

    if adj_maturities:
        df["MaturityDate"] = bday.offset(df["MaturityDate"], 0)

    df = df[["BondType", "MaturityDate", "IndicativeRate"]].copy()
    return df.sort_values(["BondType", "MaturityDate"]).reset_index(drop=True)


def tpf_pre_maturities(date: DateScalar) -> pd.Series:
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
        tpf_rates(date)
        .query("BondType in ['LTN', 'NTN-F']")["MaturityDate"]
        .drop_duplicates()
        .sort_values(ignore_index=True)
        .reset_index(drop=True)
    )
    return maturity_dates
