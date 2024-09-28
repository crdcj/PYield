import datetime as dt
import re
from typing import overload

import numpy as np
import pandas as pd

type ScalarDateTypes = str | np.datetime64 | pd.Timestamp | dt.datetime | dt.date
type PandasArrayDateTypes = pd.Series | pd.DatetimeIndex | pd.Index
type ArrayDateTypes = PandasArrayDateTypes | np.ndarray | list | tuple


def validate_year_format(value):
    """If the input date is a string, it validates the year format."""
    if not isinstance(value, str):
        return

    value = value.strip().lower()
    if value in {"today", "now"}:
        return

    if re.match(r"^\d{4}-", value):
        error_msg = f"Invalid format: {value}. Day first format is required. Example: '31-05-2024'."  # noqa
        raise ValueError(error_msg)


def check_first_value(dates):
    match dates:
        case None:
            raise ValueError("Date cannot be None.")
        case str():
            validate_year_format(dates)
        case list() | tuple() | np.ndarray() | pd.Series():
            validate_year_format(dates[0])


@overload
def convert_input_dates(dates: ScalarDateTypes) -> pd.Timestamp: ...
@overload
def convert_input_dates(dates: ArrayDateTypes) -> pd.Series: ...


def convert_input_dates(
    dates: ScalarDateTypes | ArrayDateTypes,
) -> pd.Timestamp | pd.Series:
    check_first_value(dates)

    result = pd.to_datetime(dates, dayfirst=True)

    match result:
        case pd.Timestamp():
            result = result.normalize()
        case dt.datetime() | dt.date() | np.datetime64():
            result = pd.Timestamp(result).normalize()
        case pd.Series():
            result = result.astype("datetime64[ns]")
        case pd.DatetimeIndex():
            result = pd.Series(result).astype("datetime64[ns]")
        case None:
            raise ValueError("Date cannot be None.")
        case _:
            raise ValueError("Invalid date input type.")

    return result


def convert_to_numpy_date(
    dates: pd.Timestamp | pd.Series,
) -> np.datetime64 | np.ndarray:
    """
    Converts the input dates to a numpy datetime64[D] format.

    Args:
        dates (Timestamp | Series): A single date or a Series of dates.

    Returns:
        np.datetime64 | np.ndarray: The input dates in a numpy datetime64[D] format.
    """
    if isinstance(dates, pd.Timestamp):
        return np.datetime64(dates, "D")
    else:
        return dates.to_numpy().astype("datetime64[D]")
