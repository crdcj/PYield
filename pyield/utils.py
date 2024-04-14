import pandas as pd

from . import bday


def _normalize_date(reference_date: str | pd.Timestamp | None = None) -> pd.Timestamp:
    """
    Normalizes the given date to ensure it is a past business day at midnight. If no
    date is provided, it defaults to the previous business day.

    Args:
        reference_date (str | pd.Timestamp | None): The date to normalize. Can be a
        string, pandas Timestamp or None. If None, it defaults to the last business day.

    Returns:
        pd.Timestamp: A normalized pandas Timestamp representing a past business day at
        midnight.

    Raises:
        ValueError: If the input date format is invalid, if the date is in the future,
        or if the date is not a business day.

    Notes:
        - Normalization means setting the time component of the timestamp to midnight.
        - The function checks if the normalized date is a business day and adjusts
          accordingly.
        - Business day calculations consider local market holidays.

    Examples:
        >>> _normalize_date('2023-04-01')
        >>> _normalize_date(pd.Timestamp('2023-04-01 15:30'))
        >>> _normalize_date()
    """
    if isinstance(reference_date, str):
        # Convert string date to Timestamp and normalize to midnight
        normalized_date = pd.Timestamp(reference_date).normalize()
    elif isinstance(reference_date, pd.Timestamp):
        # Normalize Timestamp to midnight
        normalized_date = reference_date.normalize()
    elif reference_date is None:
        # If no date is provided, use the previous business day
        today = pd.Timestamp.today().normalize()
        normalized_date = bday.offset_bdays(today, -1)
    else:
        raise ValueError("Invalid date format.")

    # Validate that the date is not in the future
    if normalized_date > pd.Timestamp.today().normalize():
        raise ValueError("Reference date cannot be in the future.")

    # Validate that the date is a business day
    if not bday.is_bday(normalized_date):
        raise ValueError("Reference date must be a business day.")

    return normalized_date
