import io
import os
import zipfile as zp

import pandas as pd
import requests

from .. import bday
from .. import date_converter as dc

# URL Constants
ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs"
ANBIMA_MEMBER_URL = "http://www.anbima.associados.rtm/merc_sec/arqs"
# URL example: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt
RATES_URL = (
    "https://raw.githubusercontent.com/crdcj/pyield-data/main/anbima_data.parquet"
)

# Before 13/05/2014 the file was zipped and the endpoint ended with ".exe"
FORMAT_CHANGE_DATE = pd.to_datetime("13-05-2014", dayfirst=True)


def _get_file_content(reference_date: pd.Timestamp) -> str:
    url_date = reference_date.strftime("%y%m%d")
    if reference_date < FORMAT_CHANGE_DATE:
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

        if reference_date < FORMAT_CHANGE_DATE:
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


def anbima_data(
    reference_date: str | pd.Timestamp,
    bond_type: str | None = None,
) -> pd.DataFrame:
    """
    Fetches indicative treasury rates from ANBIMA for a specified reference date.

    This function retrieves the indicative rates for Brazilian treasury securities from
    ANBIMA, processing them into a structured pandas DataFrame.

    Args:
        reference_date (str | pd.Timestamp): The date for which to fetch the indicative
            rates. If a string is provided, it should be in the format 'dd-mm-yyyy'.
        bond_type (str, optional): The type of bond to filter by. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the processed ANBIMA data for the
            given reference date.

    Examples:
        # Fetch ANBIMA data for all bonds in a specific reference date
        >>> anbima_data("18-06-2024")
        # Fetch ANBIMA data for NTN-B bonds in a specific reference date
        >>> anbima_data("18-06-2024", "NTN-B")
    """
    # Normalize the reference date
    normalized_date = dc.convert_date(reference_date)
    file_content = _get_file_content(normalized_date)

    if not file_content:
        date_str = normalized_date.strftime("%d-%m-%Y")
        raise ValueError(f"Could not fetch ANBIMA data for {date_str}.")

    df = _read_raw_df(file_content)

    df = _process_raw_df(df)

    if bond_type is None:
        return df
    else:
        df.query("BondType == @bond_type", inplace=True)
        return df.reset_index(drop=True)


class RatesData:
    _df = pd.DataFrame()

    @classmethod
    def _load_data(cls):
        # if cls._df.empty or not cls._is_data_up_to_date():
        print("Loading ANBIMA data...")
        cls._df = pd.read_parquet(RATES_URL)
        cls._last_update = pd.Timestamp.today().normalize()

    @classmethod
    def _is_data_up_to_date(cls) -> bool:
        """Check if the last date in the file is the last available ANBIMA date."""
        if cls._df.empty:
            return False
        today = pd.Timestamp.today().normalize()
        last_anbima_date = bday.offset(today, -1)
        last_file_date = cls._df["ReferenceDate"].max()
        return last_anbima_date == last_file_date

    @classmethod
    def _check_for_updates(cls):
        """Check if the data is up to date. If not, load the latest data."""
        if cls._df.empty or not cls._is_data_up_to_date():
            cls._load_data()

    @classmethod
    def _get_dataframe(cls):
        cls._load_data()
        return cls._df.copy()

    @classmethod
    def rates(
        cls,
        reference_date: pd.Timestamp | None = None,
        bond_type: str | None = None,
    ) -> pd.DataFrame:
        cls._check_for_updates()
        df = cls._df.copy()
        reference_date = dc.convert_date(reference_date)
        df.query("ReferenceDate == @reference_date", inplace=True)
        if bond_type is not None:
            df.query("BondType == @bond_type", inplace=True)

        return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)


def anbima_rates(
    reference_date: str | pd.Timestamp,
    bond_type: str | None = None,
) -> pd.DataFrame:
    """
    Gets ANBIMA indicative rates for a specified reference date and bond type.

    This function retrieves the ANBIMA indicative rates for Brazilian treasury
    securities, processing them into a structured pandas DataFrame.

    Args:
        reference_date (str | pd.Timestamp): The date for which to filter the indicative
            rates. If a string is provided, it should be in the format 'dd-mm-yyyy'.
            If None, the function returns all available rates. Defaults to None.
        bond_type (str, optional): The type of bond to filter by. If None, the function
            returns all bond types available. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the processed indicative rates for the
            given reference date.

    Examples:
        # Fetch ANBIMA rates for all bonds in a specific reference date
        >>> anbima_rates("18-06-2024")
        # Fetch ANBIMA data for NTN-B bonds in a specific reference date
        >>> anbima_rates("NTN-B", "18-06-2024")
    """

    return RatesData.rates(reference_date, bond_type)
