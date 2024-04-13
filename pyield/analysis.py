import pandas as pd

from . import treasuries as tr
from .utils import _normalize_date


def calculate_spreads(spread_type: str, reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Calculates spreads for Brazilian treasury bonds based on indicative rates.

    This function calculates the spread between different types of Brazilian treasury
    bonds based on ANBIMA's indicative rates. The available spread types are 'LTN-NTN-F'
    and 'LTN-LFT'. If no reference date is provided, the function uses the previous
    business day.

    Parameters:
        spread_type (str): The type of spread to calculate. Available options are:
            - "DI_vs_PRE": the spread between DI Futures and Treasury Pre-Fixed bonds.
        reference_date (str | pd.Timestamp, optional): The reference date for the
            spread calculation. If None or not provided, defaults to the previous
            business day according to the Brazilian calendar.

    Returns:
        pd.DataFrame: A DataFrame containing the bond types, reference date, maturity
            date, and the calculated spread in basis points. The data is sorted by bond
            type and maturity date.

    Raises:
        ValueError: If an invalid spread type is provided.
    """
    # Normalize the reference date
    normalized_date = _normalize_date(reference_date)
    if spread_type.lower() == "di_vs_pre":
        return tr.calculate_di_spreads(normalized_date)
    else:
        raise ValueError("Invalid spread type.")
