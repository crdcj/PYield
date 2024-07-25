import pandas as pd

from .. import fetchers as ft


def anbima_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch LFT Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return ft.anbima(bond_type="LFT", reference_date=reference_date)


def anbima_rates(reference_date: str | pd.Timestamp) -> pd.Series:
    """
    Fetch LFT Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.Series: A Series containing the rates indexed by maturity date.
    """
    df = anbima_data(reference_date)
    # Set MaturityDate as index
    df = df.set_index("MaturityDate")
    df.index.name = None
    # Return as Series
    return df["IndicativeRate"]
