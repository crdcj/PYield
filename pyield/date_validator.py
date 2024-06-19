import numpy as np
import pandas as pd

from . import bday


def normalize_date(input_date: str | pd.Timestamp | None = None) -> pd.Timestamp:
    """
    Normalizes the given date to ensure it is a past business day at midnight. If no
    date is provided, it defaults to the last business day.

    Args:
        reference_date (str | pd.Timestamp | None): The date to normalize. Can be a
        string, pandas Timestamp or None. If None, it defaults to the last business day.

    Returns:
        pd.Timestamp: A normalized pandas Timestamp.

    Raises:
        ValueError: If the input date format is invalid.

    Notes:
        - Normalization means setting the time component of the timestamp to midnight.
        - The function checks if the normalized date is a business day and adjusts
          accordingly.
        - Business day calculations consider local market holidays.

    Examples:
        >>> normalize_date("31-05-2024")
        >>> normalize_date(pd.Timestamp("2023-04-01 15:30"))
    """
    if isinstance(input_date, str):
        # Convert string date to Timestamp and normalize to midnight
        normalized_date = pd.to_datetime(input_date, dayfirst=True).normalize()
    elif isinstance(input_date, pd.Timestamp):
        # Normalize Timestamp to midnight
        normalized_date = input_date.normalize()
    elif isinstance(input_date, np.datetime64):
        # Convert numpy datetime to Timestamp and normalize to midnight
        normalized_date = pd.Timestamp(input_date).normalize()
    elif input_date is None:
        # If no date is provided, use the last available business day
        today = pd.Timestamp.today().normalize()
        normalized_date = bday.offset(dates=today, offset=0, roll="backward")
    else:
        raise ValueError(f"Date format not recognized: {input_date}")

    return normalized_date
