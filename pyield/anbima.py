import io

import pandas as pd
import requests

# URL Constants
ANBIMA_NON_MEMBER_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs/"
ANBIMA_MEMBER_URL = "http://www.anbima.associados.rtm/merc_sec/arqs/"


def _get_file_content(reference_date: pd.Timestamp, headers: dict = None) -> str:
    headers = headers or {}  # Default empty headers
    url_date = reference_date.strftime("%y%m%d")
    member_url = f"{ANBIMA_MEMBER_URL}ms{url_date}.txt"
    # Non member url example: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt
    non_member_url = f"{ANBIMA_NON_MEMBER_URL}ms{url_date}.txt"

    # Tries to access the member URL first
    try:
        response = requests.get(member_url, headers=headers, timeout=5)
        # Checks if the response was successful (status code 200)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException:
        # Blind attempt to access the member URL
        pass

    # If the member URL fails, tries to access the non-member URL
    try:
        response = requests.get(non_member_url, timeout=5)
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
    """
    Process raw data from ANBIMA by renaming columns and adjusting data formats.

    Parameters:
        df (pd.DataFrame): Raw DataFrame to process.

    Returns:
        pd.DataFrame: Processed DataFrame.
    """
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


def fetch_data(reference_date: pd.Timestamp, headers: dict = None) -> pd.DataFrame:
    """
    Fetches indicative treasury rates from ANBIMA for a specified reference date.

    This function retrieves the indicative rates for Brazilian treasury securities
    from ANBIMA, processing them into a structured pandas DataFrame.

    Parameters:
        reference_date (pd.Timestamp): The date for which to fetch the indicative rates.

    Returns:
        pd.DataFrame: A DataFrame containing the processed indicative rates for the
            given reference date.

    """
    file_content = _get_file_content(reference_date, headers)

    if file_content == "":
        date_str = reference_date.strftime("%d-%m-%Y")
        raise ValueError(f"Could not fetch ANBIMA data for {date_str}.")

    df = _read_raw_df(file_content)

    return _process_raw_df(df)


def fetch_rates(reference_date: pd.Timestamp, headers: dict = None) -> pd.DataFrame:
    """
    Fetches indicative treasury rates from ANBIMA for a specified reference date.

    This function retrieves the indicative rates for Brazilian treasury securities
    from ANBIMA, processing them into a structured pandas DataFrame.

    Parameters:
        reference_date (pd.Timestamp): The date for which to fetch the indicative rates.

    Returns:
        pd.DataFrame: A DataFrame containing the processed indicative rates for the
            given reference date.

    """
    df = fetch_data(reference_date, headers)
    return df[["BondType", "MaturityDate", "IndicativeRate"]].copy()
