import datetime as dt
from typing import Literal, overload

import numpy as np
import pandas as pd

from . import holidays

SingleDateTypes = str | np.datetime64 | pd.Timestamp | dt.datetime | dt.date
SeriesDateTypes = list | tuple | np.ndarray | pd.Series | pd.Index | pd.DatetimeIndex

TO_SERIES_TYPES = (list, tuple, np.ndarray, pd.Series, pd.Index, pd.DatetimeIndex)

# Initialize the BrHolidays class
br_holidays = holidays.BrHolidays()


@overload
def _normalize_input_dates(
    dates: SingleDateTypes | None,
) -> pd.Timestamp: ...


@overload
def _normalize_input_dates(
    dates: SeriesDateTypes,
) -> pd.Series: ...


def _normalize_input_dates(
    dates: SingleDateTypes | SeriesDateTypes | None = None,
) -> pd.Timestamp | pd.Series:
    if dates is None:
        return pd.Timestamp.today().normalize()
    elif isinstance(dates, str):
        return pd.to_datetime(dates, dayfirst=True).normalize()
    elif isinstance(dates, pd.Timestamp):
        return dates.normalize()
    elif isinstance(dates, (np.datetime64 | dt.datetime | dt.date)):
        return pd.Timestamp(dates).normalize()
    elif isinstance(dates, TO_SERIES_TYPES):
        result = pd.to_datetime(dates, dayfirst=True)
        return pd.Series(result).dt.normalize()
    else:
        raise ValueError("Invalid date format.")


def _convert_to_numpy_date(
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


@overload
def offset(
    dates: SingleDateTypes,
    offset: int = 0,
    roll: Literal["forward", "backward"] = "forward",
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Timestamp: ...


@overload
def offset(
    dates: SeriesDateTypes,
    offset: int = 0,
    roll: Literal["forward", "backward"] = "forward",
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


def offset(
    dates: SingleDateTypes | SeriesDateTypes,
    offset: int = 0,
    roll: Literal["forward", "backward"] = "forward",
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Timestamp | pd.Series:
    """
    Offsets the dates to the next or previous business day, considering brazilian
    holidays. This function supports both single dates and collections of dates,
    handling them intelligently to return either a single offset date or a series of
    offset dates. It is a wrapper for `numpy.busday_offset` adapted for Pandas data
    types and holiday adjustments.

    Args:
        dates (SingleDateTypes | SeriesDateTypes):
            The date(s) to offset. Can be a single date in various formats (string,
            `datetime`, `Timestamp`, etc.) or a collection of dates (list, tuple,
            `Series`, etc.). If None, the current date is used.
        offset (int): The number of business days to offset the dates. Positive for
            future dates, negative for past dates. Zero will return the same date if
            it's a business day, or the next business day otherwise.
        roll (Literal["forward", "backward"], optional): Direction to roll the date if
            it falls on a holiday or weekend. 'forward' to the next business day,
            'backward' to the previous. Defaults to 'forward'.
        holiday_list (Literal["old", "new", "infer"], optional):
            The list of holidays to consider. 'old' or 'new' use predefined lists, while
            'infer' determines the most appropriate list based on the input dates.
            Defaults to "infer".

    Returns:
        pd.Timestamp | pd.Series: If a single date is provided, returns a single
            `Timestamp` of the offset date. If a series of dates is provided, returns a
            `Series` of offset dates.

    Examples:
        >>> date = "2023-12-23"  # Saturday before Christmas
        >>> bday.offset(date, 0)
        Timestamp('2023-12-26')

        >>> date = "2023-12-22"  # Friday before Christmas
        >>> bday.offset(date, 0)
        Timestamp('2023-12-22') # No offset because it's a business day

        >>> bday.offset(date, 1)
        Timestamp('2023-12-26') # Offset to the next business day

        >>> bday.offset(date, -1)
        Timestamp('2023-12-21') # Offset to the previous business day

    Note:
        This function uses `numpy.busday_offset` under the hood, which means it follows
        the same conventions and limitations for business day calculations. For detailed
        information on error handling and behavior, refer to the `numpy.busday_offset`
        documentation: https://numpy.org/doc/stable/reference/generated/numpy.busday_offset.html
    """
    normalized_dates = _normalize_input_dates(dates)

    selected_holidays = br_holidays.get_applicable_holidays(
        normalized_dates, holiday_list
    )
    selected_holidays_np = _convert_to_numpy_date(selected_holidays)

    dates_np = _convert_to_numpy_date(normalized_dates)
    offsetted_dates_np = np.busday_offset(
        dates_np, offsets=offset, roll=roll, holidays=selected_holidays_np
    )
    if isinstance(offsetted_dates_np, np.datetime64):
        result = pd.Timestamp(offsetted_dates_np)
        # Force to datetime[ns] if the input was a single date
        result = result.as_unit("ns")
    else:
        result_dti = pd.to_datetime(offsetted_dates_np)
        # Force the result to be a Series if the input was not a single date
        result = pd.Series(result_dti).astype("datetime64[ns]")

    return result


@overload
def count(
    start: SingleDateTypes,
    end: SingleDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> int: ...


@overload
def count(
    start: SeriesDateTypes,
    end: SingleDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


@overload
def count(
    start: SingleDateTypes,
    end: SeriesDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


@overload
def count(
    start: SeriesDateTypes,
    end: SeriesDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


def count(
    start: SingleDateTypes | SeriesDateTypes,
    end: SingleDateTypes | SeriesDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> int | pd.Series:
    """
    Counts the number of business days between a `start` date (inclusive) and an `end`
    date (exclusive). The function can handle single dates, arrays of dates and
    mixed inputs, returning either a single integer or a series of integers depending
    on the inputs. It accounts for specified holidays, effectively excluding them from
    the business day count.

    Args:
        start (SingleDateTypes | SeriesDateTypes): The start date(s)
            for counting. If None, the current date is used.
        end (SingleDateTypes | SeriesDateTypes| None, optional): The end date(s) for
            counting, which are excluded from the count themselves. If None, the current
            date is used.
        holiday_list (Literal["old", "new", "infer"], optional):
            Specifies which set of holidays to consider in the count. 'old' or 'new'
            refer to predefined holiday lists, while 'infer' automatically selects the
            list based on the most recent date in the input. Defaults to "infer".

    Returns:
        int | pd.Series: Returns an integer if `start` and `end` are single dates,
            or a Series if any of them is an array of dates.

    Notes:
        - This function is a wrapper around `numpy.busday_count`, adapted to work
          directly with various Pandas and Numpy date formats.
        - It supports flexible date inputs, including single dates, lists, Series, and
          more, for both `start` and `end` parameters.
        - The return type depends on the input types: single dates return an int, while
          arrays of dates return a pd.Series with the count for each date range.
        - See `numpy.busday_count` documentation for more details on how holidays are
          handled and how business day counts are calculated:
          https://numpy.org/doc/stable/reference/generated/numpy.busday_count.html.

    Examples:
        >>> bday.count("2023-12-15", "2024-01-01")
        10
        >>> bday.count(start="01-01-2023", end=["31-01-2023", "01-03-2023"])
        pd.Series([22, 40], dtype='int64')
    """
    normalized_start = _normalize_input_dates(start)
    normalized_end = _normalize_input_dates(end)

    # Determine which list of holidays to use
    selected_holidays = br_holidays.get_applicable_holidays(
        normalized_start, holiday_list
    )
    selected_holidays_np = _convert_to_numpy_date(selected_holidays)

    # Convert inputs to numpy datetime64[D] before calling numpy.busday_count
    start_np = _convert_to_numpy_date(normalized_start)
    end_np = _convert_to_numpy_date(normalized_end)

    result_np = np.busday_count(start_np, end_np, holidays=selected_holidays_np)
    if isinstance(result_np, np.int64):
        return int(result_np)
    else:
        return pd.Series(result_np, dtype="Int64")


def generate(
    start: SingleDateTypes | None = None,
    end: SingleDateTypes | None = None,
    inclusive: Literal["both", "neither", "left", "right"] = "both",
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series:
    """
    Generates a Series of business days between a `start` and `end` date, considering
    the list of Brazilian holidays. It supports customization of holiday lists and
    inclusion options for start and end dates. It wraps `pandas.bdate_range`.

    Args:
        start (SingleDateTypes | None, optional):
            The start date for generating business days. If None, the current date is
            used. Defaults to None.
        end (SingleDateTypes | None, optional):
            The end date for generating business days. If None, the current date is
            used. Defaults to None.
        inclusive (Literal["both", "neither", "left", "right"], optional):
            Determines which of the start and end dates are included in the result.
            Valid options are 'both', 'neither', 'left', 'right'. Defaults to 'both'.
        holiday_list (Literal["old", "new", "infer"], optional):
            Specifies the list of holidays to consider. 'old' or 'new' refer to
            predefined lists, 'infer' selects the list based on the most recent date in
            the range. Defaults to "infer".

    Returns:
        pd.Series: A Series representing a range of business days between the specified
            start and end dates, considering the specified holidays.

    Examples:
        >>> bday.generate(start="22-12-2023", end="02-01-2024")
        pd.Series(['2023-12-22', '2023-12-26', '2023-12-27', '2023-12-28', '2023-12-29',
            '2024-01-02'], dtype='datetime64[ns]')

    Note:
        For detailed information on parameters and error handling, refer to
        `pandas.bdate_range` documentation:
        https://pandas.pydata.org/docs/reference/api/pandas.bdate_range.html.
    """
    normalized_start = _normalize_input_dates(start)
    normalized_end = _normalize_input_dates(end)

    selected_holidays = br_holidays.get_applicable_holidays(
        normalized_start, holiday_list
    )
    selected_holidays_list = selected_holidays.to_list()

    # Get the result as a DatetimeIndex (dti)
    result_dti = pd.bdate_range(
        start=normalized_start,
        end=normalized_end,
        freq="C",
        inclusive=inclusive,
        holidays=selected_holidays_list,
    )
    return pd.Series(result_dti.values)


def is_business_day(date: SingleDateTypes | None = None) -> bool:
    """
    Checks if the input date is a business day.

    Args:
        date (SingleDateTypes | None, optional): The date to check.
            If None, the current date is used. Defaults to None.

    Returns:
        bool: True if the input date is a business day, False otherwise.

    Examples:
        >>> bday.is_business_day("25-12-2023")  # Christmas
        False
        >>> bday.is_business_day()  # Check if today is a business day
        True
    """
    normalized_date = _normalize_input_dates(date)
    # Shift the date if it is not a business day
    adjusted_date = offset(normalized_date, 0)
    return normalized_date == adjusted_date
