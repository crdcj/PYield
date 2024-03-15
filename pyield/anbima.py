import pandas as pd
from urllib.error import HTTPError


def get_anbima_rates(reference_date: str | pd.Timestamp | None = None) -> pd.DataFrame:
    """
    Fetch indicative rates from ANBIMA for a specific date.

    Parameters:
    - reference_date: pd.Timestamp for which to fetch the indicative rates.
        If None, fetches yesterday's rates.

    Returns:
    - A pandas DataFrame with the indicative rates for the given date.
    """
    if reference_date:
        # Force reference_date to be a pd.Timestamp
        reference_date = pd.Timestamp(reference_date)
    else:  # If no reference_date is given, use yesterday's date
        today = pd.Timestamp.today().date()
        yesterday = today - pd.Timedelta(days=1)
        reference_date = yesterday

    # Example URL: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms231128.txt
    url_date = reference_date.strftime("%y%m%d")
    url = f"https://www.anbima.com.br/informacoes/merc-sec/arqs/ms{url_date}.txt"

    try:
        df = pd.read_csv(
            url, sep="@", encoding="latin1", skiprows=2, decimal=",", thousands="."
        )
    except HTTPError:
        error_date = reference_date.strftime("%d/%m/%Y")
        raise ValueError(f"Failed to get ANBIMA rates for {error_date}")

    # Filter selected columns and rename them
    selected_columns_dict = {
        "Titulo": "BondType",
        "Data Vencimento": "MaturityDate",
        "Tx. Indicativas": "IndicativeRate",
    }
    df = df[list(selected_columns_dict.keys())]
    df.rename(columns=selected_columns_dict, inplace=True)

    df["MaturityDate"] = pd.to_datetime(df["MaturityDate"], format="%Y%m%d")
    df.insert(0, "ReferenceDate", reference_date)

    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)
