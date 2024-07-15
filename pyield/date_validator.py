import re

import numpy as np
import pandas as pd

from . import bday


def _starts_with_year(date_str: str) -> bool:
    """Check if the date string starts with a four-digit year."""
    if date_str in {"today", "now"}:
        return False
    else:
        return bool(re.match(r"^\d{4}-", date_str))


def standardize_date(
    input_date: str | pd.Timestamp | np.datetime64 | None = None,
) -> pd.Timestamp:
    """
    Normalizes the given date to ensure it is a business day at midnight. If no date is
    provided, it defaults to the last business day available.

    Args:
        reference_date (str | pd.Timestamp | np.datetime64 | None): The date to
        normalize. If None, it defaults to the last business day. If str, it should be
        with day first format (e.g. "31-05-2024").

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
    match input_date:
        case str():
            if _starts_with_year(input_date):
                raise ValueError(
                    f"Invalid date format: {input_date}. Day first format is required (e.g. '31-05-2024')."  # noqa
                )
            # Parse the date string with day first format
            output_date = pd.to_datetime(input_date, dayfirst=True)
        case pd.Timestamp():
            # Use Timestamp as is
            output_date = input_date
        case np.datetime64():
            # Convert numpy datetime to pandas Timestamp
            output_date = pd.Timestamp(input_date)
        case None:
            # If no date is provided, use the last business day available
            today = pd.Timestamp.today()
            output_date = bday.offset(dates=today, offset=0, roll="backward")
        case _:
            raise ValueError(f"Date format not recognized: {input_date}")
    # Normalize the final date to midnight
    return output_date.normalize()
