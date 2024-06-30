import pandas as pd

from .fetchers.anbima import anbima


def anbima_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-B Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return anbima(bond_type="LTN", reference_date=reference_date)


def indicative_rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-B Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the maturity dates and corresponding rates.
    """
    df = anbima_data(reference_date)

    # Keep only the relevant columns for the output
    keep_columns = ["ReferenceDate", "BondType", "MaturityDate", "IndicativeRate"]
    return df[keep_columns].copy()
