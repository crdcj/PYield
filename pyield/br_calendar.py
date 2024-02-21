from pathlib import Path
from typing import Literal
import pandas as pd
import numpy as np

new_holidays_path = Path(__file__).parent / "new_br_holidays.txt"
old_holidays_path = Path(__file__).parent / "old_br_holidays.txt"

df_new = pd.read_csv(new_holidays_path, header=None, names=["date"], comment="#")
df_old = pd.read_csv(old_holidays_path, header=None, names=["date"], comment="#")

df_new["date"] = pd.to_datetime(df_new["date"], format="%d/%m/%Y")
df_old["date"] = pd.to_datetime(df_old["date"], format="%d/%m/%Y")

# Using numpy datetime64[D] array increases performance by almost 10x
NEW_BR_HOLIDAYS = df_new["date"].values.astype("datetime64[D]")
OLD_BR_HOLIDAYS = df_old["date"].values.astype("datetime64[D]")


def offset_bdays(
    dates: str | pd.Timestamp, offset: int, holiday_list: Literal["old", "new"] = "new"
):
    """
    Offsets the dates to the next or previous business day. This function is a wrapper
    for `numpy.busday_offset` to be used directly with Pandas data types that takes into
    account the new list of brazilian holidays as the default.

    Args:
        dates (str | pd.Timestamp | pd.Series): A single date or a Series of dates to be offset.
        offset (int): The number of business days to offset the dates. Positive numbers
            offset to the next business day, negative numbers offset to the previous
            business day. Zero offsets to the same date if it's a business day, otherwise
            offsets to the next business day.
        holiday_list (str, optional): Defaults to "new". The list of holidays to use.

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
    # Convert to pandas Series of datetime64[ns] even if a single value was passed
    dates = pd.to_datetime(pd.Series(dates))

    # Convert to numpy data types
    dates = dates.values.astype("datetime64[D]")

    # Get the correct list of holidays from passed argument
    holiday_list = NEW_BR_HOLIDAYS if holiday_list == "new" else OLD_BR_HOLIDAYS

    # Adjust the dates according to the offset
    adj_dates = np.busday_offset(
        dates, offsets=offset, roll="forward", holidays=holiday_list
    )
    # Convert back to pandas datetime64[ns]
    adj_dates = pd.to_datetime(adj_dates, unit="ns")
    # Convert back to Series
    adj_dates = pd.Series(adj_dates)

    # Return a single value if a single value was passed
    if len(adj_dates) == 1:
        adj_dates = adj_dates[0]

    return adj_dates


def count_bdays(start, end, holiday_list: Literal["old", "new"] = None):
    """
    Counts the number of business days between a `start` (inclusive) and `end`
    (exclusive). If an end date is earlier than the start date, the count will be
    negative. This function is a wrapper for `numpy.busday_count` to be used directly
    with Pandas data types. The start date is used to determine which list of holidays
    to use (see Notes for more information on this).

    Args:
        start (str | pd.Timestamp, optional): Defaults to None. The start date.
        end (str | pd.Timestamp optional): Defaults to None. The end date.

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
    start = pd.to_datetime(pd.Series(start))
    end = pd.to_datetime(pd.Series(end))

    # Convert to numpy data types
    start = start.values.astype("datetime64[D]")
    end = end.values.astype("datetime64[D]")

    # Determine which list of holidays to use
    if holiday_list is None:
        cutoff_date = np.datetime64("2023-12-26", "D")
        # Check if cutoff_date is between min and max dates in start
        if start.min() < cutoff_date <= start.max():
            raise ValueError(
                "Cannot determine which list of holidays to use. Please specify the list of holidays."
            )

        if start.max() < cutoff_date:
            holiday_list = "old"
        else:
            holiday_list = "new"

    bdays = np.busday_count(start, end, holidays=holiday_list)

    # Return a single value if a single value was passed
    if len(bdays) == 1:
        bdays = bdays[0]

    return bdays


def generate_bdays(
    start=None,
    end=None,
    inclusive="both",
    holiday_list: Literal["old", "new"] = "new",
    **kwargs,
):
    """
    Generates a Series of business days between a `start` (inclusive) and
    `end` (inclusive) that takes into account the list of brazilian holidays as the
    default. If no start date is provided, the current date is used. If no end date is
    provided, the current date is used.


    Args:
        start (str | pd.Timestamp, optional): Defaults to None. The start date.
        end (str | pd.Timestamp | pd.Series, optional): Defaults to None. The end date.
        inclusive (str, optional): Defaults to 'both'. Whether to include the start and
            end dates. Valid options are 'both', 'neither', 'left', 'right'.
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
    if start is None and end is None:
        raise ValueError("start and end dates must be provided")
    if start is None or end is None:
        if start:
            end = pd.Timestamp.today()
        else:
            start = pd.Timestamp.today()

    # Get the correct list of holidays from passed argument
    holiday_list = NEW_BR_HOLIDAYS if holiday_list == "new" else OLD_BR_HOLIDAYS

    bdays = pd.bdate_range(
        start, end, freq="C", inclusive=inclusive, holidays=holiday_list, **kwargs
    )

    return pd.Series(bdays)
