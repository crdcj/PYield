import datetime as dt
import re
from typing import overload

import numpy as np
import pandas as pd

type ScalarDateTypes = str | np.datetime64 | pd.Timestamp | dt.datetime | dt.date
type PandasArrayDateTypes = pd.Series | pd.DatetimeIndex | pd.Index
type ArrayDateTypes = PandasArrayDateTypes | np.ndarray | list | tuple


def validate_year_format(value) -> None:
    """If the input date is a string, it validates the year format."""
    value = str(value)
    value = value.strip().lower()
    if value in {"today", "now"}:
        return

    if re.match(r"^\d{4}-", value):
        error_msg = f"Invalid format: {value}. Day first format is required. Example: '31-05-2024'."  # noqa
        raise ValueError(error_msg)


@overload
def convert_input_dates(dates: ScalarDateTypes) -> pd.Timestamp: ...
@overload
def convert_input_dates(dates: ArrayDateTypes) -> pd.Series: ...


def convert_input_dates(
    dates: ScalarDateTypes | ArrayDateTypes,
) -> pd.Timestamp | pd.Series:
    match dates:
        case None:
            raise ValueError("'dates' cannot be None.")

        case str():
            validate_year_format(dates)
            return pd.to_datetime(dates, dayfirst=True)

        case dt.datetime() | dt.date() | np.datetime64() | pd.Timestamp():
            return pd.Timestamp(dates).normalize()

        case pd.Series() | pd.Index() | np.ndarray() | list() | tuple():
            result = pd.Series(dates)

            if result.empty:
                raise ValueError("'dates' cannot be an empty Array.")

            if pd.api.types.is_string_dtype(result):
                validate_year_format(result[0])

            return pd.to_datetime(result, dayfirst=True).astype("datetime64[ns]")

        case pd.DatetimeIndex():
            return pd.Series(result).astype("datetime64[ns]")

        case _:
            raise ValueError("Invalid input type for 'dates'.")


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
