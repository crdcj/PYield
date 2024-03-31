from typing import Literal

import numpy as np
import pandas as pd
from pandas import Series, Timestamp
from pandas._libs.tslibs.nattype import NaTType

from .br_holidays import BrHolidays

# Instância global da classe Holidays
br_holidays = BrHolidays()


def format_input_date(date: str | Timestamp | None) -> Timestamp:
    """
    Formats the input date to a pandas Timestamp. If the input date is None, the
    current date is used.

    Args:
        date (str | pd.Timestamp | None): A single date or a Series of dates.

    Returns:
        pd.Timestamp: The input date in a pandas Timestamp format.
    """
    if date:
        result = pd.to_datetime(date)
    else:
        result = pd.Timestamp.today().normalize()

    if result is pd.NaT:
        raise ValueError("Invalid date format.")

    return result


def format_input_dates(dates: Series) -> Series:
    """
    Formats the input dates to a pandas Series of Timestamps.

    Args:
        dates (pd.Series): A Series of dates.

    Returns:
        pd.Series: The input dates in a pandas Series of Timestamps format.
    """
    result = pd.to_datetime(dates)
    if result is pd.NaT:
        raise ValueError("Invalid date format.")
    return result


def convert_to_numpy_date(dates: Timestamp | Series) -> np.datetime64 | np.ndarray:
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


def offset_bdays(
    dates: str | Timestamp | Series | None,
    offset: int,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> Timestamp | Series | NaTType:
    """
    Offsets the dates to the next or previous business day. This function is a wrapper
    for `numpy.busday_offset` to be used directly with Pandas data types that infers the
    right list of holidays based on the most recent date in the input.

    Args:
        dates (str | pd.Timestamp | pd.Series): A single date or a Series of dates to be offset.
        offset (int): The number of business days to offset the dates. Positive numbers
            offset to the next business day, negative numbers offset to the previous
            business day. Zero offsets to the same date if it's a business day, otherwise
            offsets to the next business day.
        holiday_list (str, optional): The list of holidays to use. Defaults to "infer", which
            infers the right list of holidays based on the most recent date in the input.

    Returns:
        pd.Timestamp | pd.Series: The offset dates. Returns a single date if
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
    if isinstance(dates, Series):
        formatted_dates = format_input_dates(dates)
    elif isinstance(dates, str | Timestamp | None):
        formatted_dates = format_input_date(dates)
    else:
        raise ValueError("Invalid input date format.")

    selected_holidays = br_holidays.get_applicable_holidays(
        formatted_dates, holiday_list
    )
    selected_holidays_np = convert_to_numpy_date(selected_holidays)

    dates_np = convert_to_numpy_date(formatted_dates)
    offsetted_dates_np = np.busday_offset(
        dates_np, offsets=offset, roll="forward", holidays=selected_holidays_np
    )
    if isinstance(offsetted_dates_np, np.datetime64):
        return pd.Timestamp(offsetted_dates_np, unit="ns")
    else:
        result = pd.to_datetime(offsetted_dates_np, unit="ns")
        return pd.Series(result)


def count_bdays(
    start: str | Timestamp | Series | None = None,
    end: str | Timestamp | Series | None = None,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> int | Series:
    """
    Counts the number of business days between a `start` (inclusive) and `end`
    (exclusive). If an end date is earlier than the start date, the count will be
    negative. This function is a wrapper for `numpy.busday_count` to be used directly
    with Pandas data types.

    Args:
        start (str | Timestamp, optional): The start date. Defaults to None.
        end (str | Timestamp optional): The end date. Defaults to None.
        holiday_list (str, optional): The list of holidays to use. Defaults to "infer",
            which infers the right list of holidays based on the most recent date in
            the input.

    Returns:
        int | pd.Series: The number of business days between the start date and end date.
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
    if isinstance(start, Series):
        formatted_start = format_input_dates(start)
    else:
        formatted_start = format_input_date(start)

    if isinstance(end, Series):
        formatted_end = format_input_dates(end)
    else:
        formatted_end = format_input_date(end)

    # Determine which list of holidays to use
    selected_holidays = br_holidays.get_applicable_holidays(
        formatted_start, holiday_list
    )
    selected_holidays_np = convert_to_numpy_date(selected_holidays)

    # Convert inputs to numpy datetime64[D] before calling numpy.busday_count
    start_np = convert_to_numpy_date(formatted_start)
    end_np = convert_to_numpy_date(formatted_end)

    result_np = np.busday_count(start_np, end_np, holidays=selected_holidays_np)
    if isinstance(result_np, np.int64):
        return result_np
    else:
        return pd.Series(result_np)


def generate_bdays(
    start: str | Timestamp | None = None,
    end: str | Timestamp | None = None,
    inclusive="both",
    holiday_list: Literal["old", "new", "infer"] = "infer",
    **kwargs,
) -> Series:
    """
    Generates a Series of business days between a `start` (inclusive) and `end`
    (inclusive) that takes into account the list of brazilian holidays as the default.
    If no start date is provided, the current date is used. If no end date is provided,
    the current date is used.


    Args:
        start (str | pd.Timestamp, optional): The start date. Defaults to None.
        end (str | pd.Timestamp | pd.Series, optional): The end date. Defaults to None.
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
    formatted_start = format_input_date(start)
    formatted_end = format_input_date(end)

    selected_holidays = br_holidays.get_applicable_holidays(
        formatted_start, holiday_list
    )
    selected_holidays_list = selected_holidays.to_list()

    result = pd.bdate_range(
        formatted_start,
        formatted_end,
        freq="C",
        inclusive=inclusive,
        holidays=selected_holidays_list,
        **kwargs,
    )
    return pd.Series(result)
