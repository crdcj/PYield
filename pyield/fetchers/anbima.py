import io
import os

import pandas as pd
import requests

from .. import date_validator as dv

# URL Constants
ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs/"
# URL example: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt


def _get_file_content(reference_date: pd.Timestamp) -> str:
    url_date = reference_date.strftime("%y%m%d")
    filename = f"ms{url_date}.txt"

    # Tries to access the member URL first
    try:
        anbima_base_url = os.getenv("ANBIMA_BASE_URL")
        if anbima_base_url:
            anbima_member_url = f"{anbima_base_url}/merc_sec/arqs/"
            anbima_headers = {"private-token": os.getenv("ANBIMA_TOKEN")}
        else:
            anbima_member_url = "http://www.anbima.associados.rtm/merc_sec/arqs/"
            anbima_headers = None

        file_url = f"{anbima_member_url}{filename}"
        response = requests.get(file_url, headers=anbima_headers, timeout=5)
        # Checks if the response was successful (status code 200)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException:
        # Blind attempt to access the member URL
        pass

    # If the member URL fails, tries to access the non-member URL
    try:
        file_url = f"{ANBIMA_URL}{filename}"
        response = requests.get(file_url, timeout=5)
        response.raise_for_status()  # Checks if the second attempt was successful
        return response.text
    except requests.exceptions.RequestException:
        # Both URLs failed
        return ""


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

    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)


def anbima(
    bond_type: str | list[str] | None = None,
    reference_date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Fetches indicative treasury rates from ANBIMA for a specified reference date.

    This function retrieves the indicative rates for Brazilian treasury securities from
    ANBIMA, processing them into a structured pandas DataFrame.

    Args:
        bond_type (str, optional): The type of bond to filter by. Defaults to None.
        reference_date (str | pd.Timestamp | None, optional): The date for which to
            fetch the indicative rates. If a string is provided, it should be in the
            format 'dd-mm-yyyy'. Defaults last business day if None.

    Returns:
        pd.DataFrame: A DataFrame containing the processed indicative rates for the
        given reference date.

    Raises:
        ValueError: If the data could not be fetched for the given reference date or if
        an unsupported bond type is provided.

    Examples:
        # Fetch ANBIMA data for all bonds using the last business day available
        >>> yd.anbima("18-06-2024")
        # Fetch ANBIMA data for NTN-B bonds using a specific reference date
        >>> yd.anbima("NTN-B", "18-06-2024")
    """
    # Normalize the reference date
    normalized_date = dv.normalize_date(reference_date)
    file_content = _get_file_content(normalized_date)

    if not file_content:
        date_str = normalized_date.strftime("%d-%m-%Y")
        raise ValueError(f"Could not fetch ANBIMA data for {date_str}.")

    df = _read_raw_df(file_content)

    df = _process_raw_df(df)

    # If bond type is not specified, return the full DataFrame
    if bond_type is None:
        return df

    # Format bond type as a list of uppercase strings
    if isinstance(bond_type, str):
        bond_list = [bond_type]
    elif isinstance(bond_type, list):
        bond_list = bond_type
    else:
        msg = "Invalid bond type format. Must be a string or a list of strings."
        raise ValueError(msg)
    bond_list = [bt.upper() for bt in bond_list]

    return df.query("BondType in @bond_list").reset_index(drop=True)
