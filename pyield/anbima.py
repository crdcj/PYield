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


def data(reference_date: DateScalar, bond_type: str | None = None) -> pd.DataFrame:
    # Normalize the reference date
    reference_date = dc.convert_input_dates(reference_date)
    file_content = _get_file_content(reference_date)

    if not file_content:
        return pd.DataFrame()

    df = _read_raw_df(file_content)

    df = _process_raw_df(df)

    if bond_type is None:
        return df
    else:
        df.query("BondType == @bond_type", inplace=True)
        return df.reset_index(drop=True)


def rates(
    reference_date: DateScalar,
    bond_type: str | None = None,
    adj_maturities: bool = False,
) -> pd.DataFrame:
    df = get_anbima_dataset()

    reference_date = dc.convert_input_dates(reference_date)
    df.query("ReferenceDate == @reference_date", inplace=True)

    if df.empty:
        # Try to fetch the data from the Anbima website
        df = data(reference_date)

    if df.empty:
        # If the data is still empty, return an empty DataFrame
        return pd.DataFrame()

    if bond_type:
        df.query("BondType == @bond_type", inplace=True)

    if adj_maturities:
        df["MaturityDate"] = bday.offset(df["MaturityDate"], 0)

    df = df[["BondType", "MaturityDate", "IndicativeRate"]].copy()
    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)


def pre_maturities(reference_date: DateScalar) -> pd.Series:
    df = get_anbima_dataset()
    reference_date = dc.convert_input_dates(reference_date)
    df.query("ReferenceDate == @reference_date", inplace=True)
    df.query("BondType in ['LTN', 'NTN-F']", inplace=True)
    maturity_dates = df["MaturityDate"].drop_duplicates()

    return maturity_dates.sort_values(ignore_index=True)
