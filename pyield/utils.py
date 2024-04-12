import pandas as pd

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
