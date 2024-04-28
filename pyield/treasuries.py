import io

import pandas as pd
import requests

# URL Constants
ANBIMA_NON_MEMBER_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs/"
ANBIMA_MEMBER_URL = "http://www.anbima.associados.rtm/merc_sec/arqs/"


def _get_anbima_content(reference_date: pd.Timestamp) -> str:
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
        return ""


def _get_raw_df(reference_date: pd.Timestamp) -> pd.DataFrame:
    url_content = _get_anbima_content(reference_date)
    if url_content == "":
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


def _process_raw_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Process raw data from ANBIMA by filtering selected columns, renaming them and
    adjusting data formats.

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


def fetch_bonds(reference_date: pd.Timestamp, return_raw=False) -> pd.DataFrame:
    """
    Fetches indicative treasury rates from ANBIMA for a specified reference date.

    This function retrieves the indicative rates for Brazilian treasury securities
    from ANBIMA, processing them into a structured pandas DataFrame.
    There is an option to return raw data directly from the source without processing.

    Parameters:
        reference_date (str | pd.Timestamp, optional): The date for which to fetch the
            indicative rates. If None or not provided, the function defaults to the
            previous business day.
        return_raw (bool, optional): Flag to return raw data without processing.
            Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the processed indicative rates for the
            given reference date, or raw data if `return_raw` is True. The processed
            data includes bond type, reference date, maturity date and various rates
            (bid, ask, indicative) among others, depending on the `return_raw` flag.

    """
    df = _get_raw_df(reference_date)

    if not return_raw:
        df = _process_raw_df(df)

    return df
