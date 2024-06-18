import io

import pandas as pd
import requests

from . import date_validator as dv

# URL Constants
ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs/"
# URL example: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt


def _get_file_content(reference_date: pd.Timestamp, remote_access: dict = None) -> str:
    url_date = reference_date.strftime("%y%m%d")
    filename = f"ms{url_date}.txt"

    # Tries to access the member URL first
    try:
        if remote_access:
            anbima_url = f'{remote_access["anbima_url"]}/merc_sec/arqs/'
            headers = remote_access["headers_dict"]
        else:
            anbima_url = "http://www.anbima.associados.rtm/merc_sec/arqs/"
            headers = None

        file_url = f"{anbima_url}{filename}"
        response = requests.get(file_url, headers=headers, timeout=5)
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


def data(
    reference_date: str | pd.Timestamp | None = None,
    bond_type: str = None,
    remote_access: dict = None,
) -> pd.DataFrame:
    """
    Fetches indicative treasury rates from ANBIMA for a specified reference date.

    This function retrieves the indicative rates for Brazilian treasury securities from
    ANBIMA, processing them into a structured pandas DataFrame.

    Args:
        reference_date (str | pd.Timestamp | None, optional): The date for which to
            fetch the indicative rates. If a string is provided, it should be in the
            format 'dd-mm-yyyy'. Defaults last business day if None.
        bond_type (str, optional): The type of bond to filter by. Defaults to None.
        remote_access (dict, optional): Dictionary containing remote access parameters.
            Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the processed indicative rates for the
        given reference date.

    Raises:
        ValueError: If the data could not be fetched for the given reference date or if
        an unsupported bond type is provided.

    Examples:
        >>> yd.anbima.data("18-06-2024")
        >>> yd.anbima.data("18-06-2024", "NTN-B")
    """
    # Normalize the reference date
    normalized_date = dv.normalize_date(reference_date)
    file_content = _get_file_content(normalized_date, remote_access)

    if not file_content:
        date_str = normalized_date.strftime("%d-%m-%Y")
        raise ValueError(f"Could not fetch ANBIMA data for {date_str}.")

    df = _read_raw_df(file_content)

    df = _process_raw_df(df)

    # Filter by bond type if specified
    if bond_type:
        df = df.query(f"BondType == '{bond_type.upper()}'").reset_index(drop=True)

    return df


def rates(
    reference_date: str | pd.Timestamp | None = None,
    bond_type: str = None,
    remote_access: dict = None,
) -> pd.DataFrame:
    """
    Fetches indicative treasury rates from ANBIMA for a specified reference date.

    This function retrieves the indicative rates for Brazilian treasury securities
    from ANBIMA, processing them into a structured pandas DataFrame.

    Args:
        reference_date (str | pd.Timestamp | None, optional): The date for which to
            fetch the indicative rates. If a string is provided, it should be in the
            format 'dd-mm-yyyy'. Defaults to the last business day if None.
        bond_type (str, optional): The type of bond to filter by. Defaults to None.
        remote_access (dict, optional): Dictionary containing remote access parameters.
            Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the processed indicative rates for the
        given reference date.

    Examples:
        >>> yd.anbima.rates("18-06-2024")
        >>> yd.anbima.rates("18-06-2024", "NTN-B")
    """
    # Fetch the data from ANBIMA
    df = data(reference_date, bond_type, remote_access)

    # Keep only the relevant columns for the output
    keep_columns = ["ReferenceDate", "BondType", "MaturityDate", "IndicativeRate"]
    return df[keep_columns].copy()
