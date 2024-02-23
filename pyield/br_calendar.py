from pathlib import Path
from typing import Literal
import pandas as pd
import numpy as np


def get_holidays_list(file_path: Path) -> np.array:
    df = pd.read_csv(file_path, header=None, names=["date"], comment="#")
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
    # Using numpy datetime64[D] array increases performance by almost 10x
    return df["date"].values.astype("datetime64[D]")


CURRENT_DIR = Path(__file__).parent
NEW_HOLIDAYS = get_holidays_list(CURRENT_DIR / "new_br_holidays.txt")
OLD_HOLIDAYS = get_holidays_list(CURRENT_DIR / "old_br_holidays.txt")
NEW_HOLIDAYS_START_DATE = np.datetime64("2023-12-26", "D")


def convert_to_np_array(dates: pd.Series | pd.Timestamp | str) -> np.array:
    """
    Converts a Series of dates, a single date or a string to a numpy datetime64[D]
    array.

    Args:
        dates (pd.Series | pd.Timestamp | str): A Series of dates, a single date or a
        string in ISO format.

    Returns:
        np.datetime64: A numpy datetime64[D] array.

    Examples:
        >>> dates = pd.to_datetime(['2023-12-20', '2023-12-21'])
        >>> yd.convert_to_np_array(dates)
        array(['2023-12-20', '2023-12-21'], dtype='datetime64[D]')
        >>> date = '2023-12-20'
        >>> yd.convert_to_np_array(date)
        numpy.datetime64('2023-12-20')
        >>> date = pd.to_datetime('2023-12-20')
        >>> yd.convert_to_np_array(date)
        numpy.datetime64('2023-12-20')
    """
    # Convert to a Series of datetime64[ns] even if a single value was passed
    dates = pd.to_datetime(pd.Series(dates))

    # Return the numpy datetime64[D] array
    return dates.values.astype("datetime64[D]")


def select_holidays_list(dates: np.array, select: str) -> np.array:
    """
    Returns the correct list of holidays to use based on the most recent date in the input.

    Args:
        dates (pd.Timestamp): A single date or a Series of dates.
        select (str): The list of holidays to use. Valid options are 'old', 'new' or
            'infer'. If 'infer' is used, the list of holidays is selected based on the
            most recent (minimum) date in the input.

    Returns:
        np.array: The list of holidays to use.
    """
    match select:
        case "old":
            selected_list = OLD_HOLIDAYS
        case "new":
            selected_list = NEW_HOLIDAYS
        case "infer":
            selected_list = NEW_HOLIDAYS
            if dates.min() < NEW_HOLIDAYS_START_DATE:
                selected_list = OLD_HOLIDAYS
        case _:
            raise ValueError("Options are 'old', 'new' or 'infer'.")
    return selected_list


def offset_bdays(
    dates: str | pd.Timestamp,
    offset: int,
    holiday_list: Literal["old", "new", "infer"] = "infer",
):
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
        str | pd.Timestamp | pd.Series: The offset dates. Returns a single date if
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
    dates = convert_to_np_array(dates)

    selected_holidays = select_holidays_list(dates, holiday_list)

    # Adjust the dates according to the offset
    adj_dates = np.busday_offset(
        dates, offsets=offset, roll="forward", holidays=selected_holidays
    )

    # Convert back to pandas datetime64[ns]
    adj_dates = pd.to_datetime(adj_dates, unit="ns")
    # Convert back to Series
    adj_dates = pd.Series(adj_dates)

    # Return a single value if a single value was passed
    if len(adj_dates) == 1:
        adj_dates = adj_dates[0]

    return adj_dates


def count_bdays(start, end, holiday_list: Literal["old", "new", "infer"] = "infer"):
    """
    Counts the number of business days between a `start` (inclusive) and `end`
    (exclusive). If an end date is earlier than the start date, the count will be
    negative. This function is a wrapper for `numpy.busday_count` to be used directly
    with Pandas data types.

    Args:
        start (str | pd.Timestamp, optional): The start date. Defaults to None.
        end (str | pd.Timestamp optional): The end date. Defaults to None.
        holiday_list (str, optional): The list of holidays to use. Defaults to "infer",
            which infers the right list of holidays based on the most recent date in
            the input.

    Returns:
        np.int64 | np.ndarray: The number of business days between the start date and
        end dates. Returns a single integer if `end` is a single Timestamp, otherwise
        returns an ndarray of integers.

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
        array([22, 40])
    """
    # Convert inputs to a Series of datetime64[ns] even if a single value was passed
    start = convert_to_np_array(start)
    end = convert_to_np_array(end)

    # Determine which list of holidays to use
    selected_holidays = select_holidays_list(start, holiday_list)

    bdays = np.busday_count(start, end, holidays=selected_holidays)

    # Return a single value if a single value was passed
    if len(bdays) == 1:
        bdays = bdays[0]

    return bdays


def generate_bdays(
    start=None,
    end=None,
    inclusive="both",
    holiday_list: Literal["old", "new", "infer"] = "infer",
    **kwargs,
) -> pd.DatetimeIndex:
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
    if start is None:
        start = pd.Timestamp.today()

    if end is None:
        end = pd.Timestamp.today()

    selected_holidays = select_holidays_list(np.datetime64(start), holiday_list)

    bdays = pd.bdate_range(
        start, end, freq="C", inclusive=inclusive, holidays=selected_holidays, **kwargs
    )

    return pd.Series(bdays)
