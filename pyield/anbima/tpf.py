import io
import os
import zipfile as zp

import pandas as pd
import requests

from pyield import bday
from pyield import date_converter as dc
from pyield.data_cache import get_anbima_dataset
from pyield.date_converter import DateScalar

# URL Constants
ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs"
ANBIMA_MEMBER_URL = "http://www.anbima.associados.rtm/merc_sec/arqs"
# URL example: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt

# Before 13/05/2014 the file was zipped and the endpoint ended with ".exe"
FORMAT_CHANGE_DATE = pd.to_datetime("13-05-2014", dayfirst=True)


def _get_file_content(date: pd.Timestamp) -> str:
    url_date = date.strftime("%y%m%d")
    if date < FORMAT_CHANGE_DATE:
        filename = f"ms{url_date}.exe"
    else:
        filename = f"ms{url_date}.txt"

    # Tries to access the member URL first
    try:
        anbima_base_url = os.getenv("ANBIMA_BASE_URL")
        if anbima_base_url:
            anbima_member_url = f"{anbima_base_url}merc_sec/arqs"
            anbima_headers = {"private-token": os.getenv("ANBIMA_TOKEN")}
        else:
            anbima_member_url = ANBIMA_MEMBER_URL
            anbima_headers = None

        file_url = f"{anbima_member_url}/{filename}"
        r = requests.get(file_url, headers=anbima_headers, timeout=5)
        # Checks if the response was successful (status code 200)
        r.raise_for_status()

        if date < FORMAT_CHANGE_DATE:
            zip_file = zp.ZipFile(io.BytesIO(r.content))
            file_content = zip_file.read(zip_file.namelist()[0]).decode("latin-1")
        else:
            file_content = r.text

    except requests.exceptions.RequestException:
        # If the member URL fails, tries to access the non-member URL
        try:
            file_url = f"{ANBIMA_URL}/{filename}"
            r = requests.get(file_url, timeout=5)
            r.raise_for_status()  # Checks if the second attempt was successful
            file_content = r.text
        except requests.exceptions.RequestException:
            # Both URLs failed
            file_content = ""

    return file_content


def _read_raw_df(file_content: str) -> pd.DataFrame:
    return pd.read_csv(
        io.StringIO(file_content),
        sep="@",
        encoding="latin-1",
        skiprows=2,
        decimal=",",
        thousands=".",
        na_values=["--"],
        dtype_backend="numpy_nullable",
    )


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
    df[rate_cols] = (df[rate_cols] / 100).round(6)

    df["ReferenceDate"] = pd.to_datetime(df["ReferenceDate"], format="%Y%m%d")
    df["MaturityDate"] = pd.to_datetime(df["MaturityDate"], format="%Y%m%d")
    df["IssueBaseDate"] = pd.to_datetime(df["IssueBaseDate"], format="%Y%m%d")

    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)


def anbima_tpf_data(date: DateScalar, bond_type: str | None = None) -> pd.DataFrame:
    """Fetch and process TPF market data from ANBIMA.

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
            specified date.
    """
    # Normalize the reference date
    date = dc.convert_input_dates(date)
    file_content = _get_file_content(date)

    if not file_content:
        return pd.DataFrame()

    df = _read_raw_df(file_content)

    df = _process_raw_df(df)

    if bond_type is None:
        return df
    else:
        df.query("BondType == @bond_type", inplace=True)
        return df.reset_index(drop=True)


def anbima_tpf_rates(
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

    date = dc.convert_input_dates(date)
    df.query("ReferenceDate == @date", inplace=True)

    if df.empty:
        # Try to fetch the data from the Anbima website
        df = anbima_tpf_data(date)

    if df.empty:
        # If the data is still empty, return an empty DataFrame
        return pd.DataFrame()

    if bond_type:
        df.query("BondType == @bond_type", inplace=True)

    if adj_maturities:
        df["MaturityDate"] = bday.offset(df["MaturityDate"], 0)

    df = df[["BondType", "MaturityDate", "IndicativeRate"]].copy()
    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)


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
        anbima_tpf_rates(date)
        .query("BondType in ['LTN', 'NTN-F']")["MaturityDate"]
        .drop_duplicates()
        .sort_values(ignore_index=True)
        .reset_index(drop=True)
    )
    return maturity_dates
