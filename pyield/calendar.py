from typing import Literal, overload, Union

import numpy as np
import pandas as pd


from .holidays import BrHolidays

SingleDateType = Union[str, np.datetime64, pd.Timestamp]

SeriesDateType = Union[list, tuple, np.ndarray, pd.Series, pd.Index, pd.DatetimeIndex]

TO_TIMESTAMP_TYPES = (str, np.datetime64)
TO_SERIES_TYPES = (list, tuple, np.ndarray, pd.Series, pd.Index, pd.DatetimeIndex)

# Initialize the BrHolidays class
br_holidays = BrHolidays()


@overload
def _normalize_input_dates(
    dates: SingleDateType | None,
) -> pd.Timestamp: ...


@overload
def _normalize_input_dates(
    dates: SeriesDateType,
) -> pd.Series: ...


def _normalize_input_dates(
    dates: SingleDateType | SeriesDateType | None = None,
) -> pd.Timestamp | pd.Series:
    if dates is None:
        return pd.Timestamp.today().normalize()
    elif isinstance(dates, pd.Timestamp):
        return dates.normalize()
    elif isinstance(dates, TO_TIMESTAMP_TYPES):
        return pd.Timestamp(dates).normalize()
    elif isinstance(dates, TO_SERIES_TYPES):
        result = pd.to_datetime(dates)
        return pd.Series(result).dt.normalize()
    else:
        raise ValueError("Invalid date format.")


def is_business_day(date: SingleDateType | None = None) -> bool:
    """
    Checks if the input date is a business day.

    Args:
        date (str | Timestamp): The date to check.

    Returns:
        bool: True if the input date is a business day, False otherwise.

    Examples:
        >>> yd.is_business_day('2023-12-25') # Christmas
        False
        >>> yd.is_business_day() # Check if today is a business day
        True
    """
    normalized_date = _normalize_input_dates(date)
    # Shift the date if it is not a business day
    adjusted_date = offset_bdays(normalized_date, 0)
    return normalized_date == adjusted_date


def _convert_to_numpy_date(
    dates: pd.Timestamp | pd.Series,
) -> np.datetime64 | np.ndarray:
    """
    Converts the input dates to a numpy datetime64[D] format.

    Args:
        dates (str | pd.Timestamp | pd.Series): A single date or a Series of dates.

    Returns:
        np.datetime64 | np.ndarray: The input dates in a numpy datetime64[D] format.
    """
    if isinstance(dates, pd.Timestamp):
        return np.datetime64(dates, "D")
    else:
        return dates.to_numpy().astype("datetime64[D]")


@overload
def offset_bdays(
    dates: SingleDateType | None = None,
    offset: int = 0,
    roll: Literal["forward", "backward"] = "forward",
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Timestamp: ...


@overload
def offset_bdays(
    dates: SeriesDateType,
    offset: int = 0,
    roll: Literal["forward", "backward"] = "forward",
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


def offset_bdays(
    dates: SingleDateType | SeriesDateType | None = None,
    offset: int = 0,
    roll: Literal["forward", "backward"] = "forward",
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Timestamp | pd.Series:
    """
    Offsets the dates to the next or previous business day. This function is a wrapper
    for `numpy.busday_offset` to be used directly with Pandas data types that infers the
    right list of holidays based on the most recent date in the input.

    Args:
        dates (str | Timestamp | Series): A single date or a Series of dates to be offset. If
            None, the current date is used.
        offset (int): The number of business days to offset the dates. Positive numbers
            offset to the next business day, negative numbers offset to the previous
            business day. Zero offsets to the same date if it's a business day, otherwise
            offsets to the next business day.
        roll (str, optional): The direction to roll the date if it falls on a holiday.
            Valid options are 'forward' and 'backward'. Defaults to 'forward'.
        holiday_list (str, optional): The list of holidays to use. Defaults to "infer", which
            infers the right list of holidays based on the most recent date in the input.

    Returns:
        Timestamp | Series: The offset dates. Returns a single date if
        `dates` is a single date, otherwise returns a Series of dates.

    Note: For more information on error handling, see numpy.busday_offset documentation at
        https://numpy.org/doc/stable/reference/generated/numpy.busday_offset.html.

    Examples:
        >>> date = '2023-12-23' # Saturday before Christmas
        >>> yd.offset_bdays(date, 0)
        Timestamp('2023-12-26')
        >>> date = '2023-12-22' # Friday before Christmas
        >>> yd.offset_bdays(date, 0)
        Timestamp('2023-12-22') # No offset because it's a business day
        >>> yd.offset_bdays(date, 1)
        Timestamp('2023-12-26') # Offset to the next business day
        >>> yd.offset_bdays(date, -1)
        Timestamp('2023-12-21') # Offset to the previous business day
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
        return pd.Timestamp(offsetted_dates_np, unit="ns")
    else:
        result = pd.to_datetime(offsetted_dates_np, unit="ns")
        return pd.Series(result)


@overload
def count_bdays(
    start: SingleDateType | None = None,
    end: SingleDateType | None = None,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> int: ...


@overload
def count_bdays(
    start: SeriesDateType,
    end: SingleDateType | None,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


@overload
def count_bdays(
    start: SingleDateType | None,
    end: SeriesDateType,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


@overload
def count_bdays(
    start: SeriesDateType,
    end: SeriesDateType,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


def count_bdays(
    start: SingleDateType | SeriesDateType | None = None,
    end: SingleDateType | SeriesDateType | None = None,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> int | pd.Series:
    """
    Counts the number of business days between a `start` (inclusive) and `end`
    (exclusive). If an end date is earlier than the start date, the count will be
    negative. This function is a wrapper for `numpy.busday_count` to be used directly
    with Pandas data types.

    Args:
        start (str | Timestamp | Series, optional): The start date. If None, the current
            date is used. Defaults to None.
        end (str | Timestamp | Series, optional): The end date. If None, the current date
            is used. Defaults to None.
        holiday_list (str, optional): The list of holidays to use. Defaults to "infer",
            which infers the right list of holidays based on the most recent date in
            the input.

    Returns:
        int | Series | list: The number of business days between the start date and end date.
        Returns an integer if the result is a single value, otherwise returns a Series.

    Notes:
        - For more information on error handling, see numpy.busday_count documentation at
            https://numpy.org/doc/stable/reference/generated/numpy.busday_count.html.
        - The maximum start date is used to determine which list of holidays to use. If the
            maximum start date is earlier than 2023-12-26, the list of holidays is
            `OLD_BR_HOLIDAYS`. Otherwise, the list of holidays is `NEW_BR_HOLIDAYS`.

    Examples:
        >>> start = '2023-12-15'
        >>> end = '2024-01-01'
        >>> yd.count_bdays(start, end)
        10

        >>> start = '2023-01-01'
        >>> end = pd.to_datetime(['2023-01-31', '2023-03-01'])
        >>> yd.count_bdays(start, end)
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


def generate_bdays(
    start: SingleDateType | None = None,
    end: SingleDateType | None = None,
    inclusive: Literal["both", "neither", "left", "right"] = "both",
    holiday_list: Literal["old", "new", "infer"] = "infer",
    **kwargs,
) -> pd.Series:
    """
    Generates a Series of business days between a `start` (inclusive) and `end`
    (inclusive) that takes into account the list of brazilian holidays as the default.
    If no start date is provided, the current date is used. If no end date is provided,
    the current date is used.


    Args:
        start (str | Timestamp, optional): The start date. Defaults to None. If None,
            the current date is used.
        end (str | Timestamp | Series, optional): The end date. Defaults to None. If None,
            the current date is used.
        inclusive (str, optional): Whether to include the start and end dates.
            Valid options are 'both', 'neither', 'left', 'right'. Defaults to 'both'.
        holiday_list (str, optional): The list of holidays to use. Defaults to "infer",
            which infers the right list of holidays based on the most recent date in the
            input.
        **kwargs: Additional arguments to pass to `pandas.bdate_range`.

    Returns:
        pd.Series: A Series of business days between the start date and end date.

    Note:
        This function is a wrapper for `pandas.bdate_range`.For more information on
        error handling, see pandas.bdate_range documentation at
        https://pandas.pydata.org/docs/reference/api/pandas.bdate_range.html#

    Examples:
        >>> start = '2023-12-20'
        >>> end = '2024-01-05'
        >>> yd.generate_bdays(start, end)
        2023-12-15    2023-12-15
        2023-12-18    2023-12-18
        2023-12-19    2023-12-19
        2023-12-20    2023-12-20
        2023-12-21    2023-12-21
        2023-12-22    2023-12-22
        2023-12-27    2023-12-27
        2023-12-28    2023-12-28
        2023-12-29    2023-12-29
        dtype: object
    """
    # Check if the start and end dates are single dates
    is_start_single_date = isinstance(start, TO_TIMESTAMP_TYPES)
    is_end_single_date = isinstance(end, TO_TIMESTAMP_TYPES)
    if not is_start_single_date or not is_end_single_date:
        raise ValueError("The start and end dates must be single dates.")

    normalized_start = _normalize_input_dates(start)
    normalized_end = _normalize_input_dates(end)

    selected_holidays = br_holidays.get_applicable_holidays(
        normalized_start, holiday_list
    )
    selected_holidays_list = selected_holidays.to_list()

    result = pd.bdate_range(
        normalized_start,
        normalized_end,
        freq="C",
        inclusive=inclusive,
        holidays=selected_holidays_list,
        **kwargs,
    )
    return pd.Series(result)
