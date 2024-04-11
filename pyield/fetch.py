import pandas as pd

from . import treasuries as tr
from . import di
from . import bday


def _normalize_date(reference_date: str | pd.Timestamp | None = None) -> pd.Timestamp:
    if isinstance(reference_date, str):
        normalized_date = pd.Timestamp(reference_date).normalize()
    elif isinstance(reference_date, pd.Timestamp):
        normalized_date = reference_date.normalize()
    elif reference_date is None:
        today = pd.Timestamp.today().normalize()
        # Get last business day before today
        normalized_date = bday.offset_bdays(today, -1)
    else:
        raise ValueError("Invalid date format.")

    # Raise an error if the reference date is in the future
    if normalized_date > pd.Timestamp.today().normalize():
        raise ValueError("Reference date cannot be in the future.")

    # Raise error if the reference date is not a business day
    if not bday.is_bday(normalized_date):
        raise ValueError("Reference date must be a business day.")

    return normalized_date


def fetch_data(
    asset: str,
    reference_date: str | pd.Timestamp | None = None,
    return_raw: bool = False,
) -> pd.DataFrame:
    """
    Fetches data for a specified asset from a specified source between start and end dates.

    Args:
    asset (str): The asset type (e.g., 'Treasury', 'DI Futures').
    reference_date (str): Reference date for the data in YYYY-MM-DD format.
    return_raw (bool): If true, returns raw data without processing. Defaults to False.

    Returns:
    pd.DataFrame: A DataFrame containing the fetched data.

    Raises:
    ValueError: If the specified source or asset type is not supported.
    """
    # Validate the reference date, defaulting to the previous business day if not provided
    normalized_date = _normalize_date(reference_date)

    if asset.lower() == "treasury":
        return tr.fetch_data(normalized_date, return_raw)
    elif asset.lower() == "di futures":
        return di.fetch_data(normalized_date, return_raw)
    else:
        raise ValueError("Asset type not supported for ANBIMA.")
