import pandas as pd

from . import treasuries as tr
from .utils import _normalize_date


def calculate_spreads(spread_type: str, reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Calculates spreads between assets based on the specified spread type.
    If no reference date is provided, the function uses the previous business day.

    Parameters:
        spread_type (str): The type of spread to calculate. Available options are:
            - "DI_vs_PRE": the spread between DI Futures and Treasury Pre-Fixed bonds.
        reference_date (str | pd.Timestamp, optional): The reference date for the
            spread calculation. If None or not provided, defaults to the previous
            business day according to the Brazilian calendar.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated spread in basis points.
        The data is sorted by asset type and maturity/expiration date.

    Raises:
        ValueError: If an invalid spread type is provided.
    """
    # Normalize the reference date
    normalized_date = _normalize_date(reference_date)
    if spread_type.lower() == "di_vs_pre":
        return tr.calculate_di_spreads(normalized_date)
    else:
        raise ValueError("Invalid spread type.")
