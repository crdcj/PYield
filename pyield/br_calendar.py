from pathlib import Path
import pandas as pd
import numpy as np

new_holidays_path = Path(__file__).parent / "br_holidays.txt"
old_holidays_path = Path(__file__).parent / "br_holidays_old.txt"

df_new = pd.read_csv(new_holidays_path, header=None, names=["date"], comment="#")
df_old = pd.read_csv(old_holidays_path, header=None, names=["date"], comment="#")

df_new["date"] = pd.to_datetime(df_new["date"], format="%d/%m/%Y")
df_old["date"] = pd.to_datetime(df_old["date"], format="%d/%m/%Y")

# Using numpy datetime64[D] array increases performance by almost 10x
BR_HOLIDAYS = df_new["date"].values.astype("datetime64[D]")
# BR_HOLIDAYS = df_new["date"].dt.strftime("%Y-%m-%d").to_list()
BR_HOLIDAYS_OLD = df_old["date"].values.astype("datetime64[D]")


def adjust_to_next_business_day(
    date: str | pd.Timestamp, holiday_list: np.ndarray = BR_HOLIDAYS
) -> pd.Timestamp:
    """
    Adjusts a date to the next business day. If the date is already a business day,
    it is returned unchanged.

    Args:
        date (pd.Timestamp): A Timestamp representing the date to be adjusted.

    Returns:
        pd.Timestamp: The next business day after the input date if it is not a business
        day, otherwise returns the input date unchanged.

    Examples:
        >>> import pandas as pd
        >>> date = '2023-12-23' # Saturday before Christmas
        >>> adjust_to_next_business_day(date)
        Timestamp('2023-12-26')
    """
    date = pd.Timestamp(date)
    date = date.to_numpy().astype("datetime64[D]")

    # Adjust to next business day
    adj_date = np.busday_offset(date, 0, roll="forward", holidays=holiday_list)

    # Convert back to pandas data type
    return pd.Timestamp(adj_date)


def count_bdays(start, end):
    """
    Counts the number of business days between a `start` (inclusive) and
    `end` (exclusive). If an end date is earlier than the start date, the count
    will be negative. This function is a wrapper for `numpy.busday_count` to be used
    directly with Pandas data types. The start date is used to determine which list of
    holidays to use. Because of this, a single value for start date is necessary in order
    to use the numpy function with the right list of holidays.

    Args:
        start: a datetime-like object representing the start date.
        end: a datetime Series or datetime-like object representing the end date(s).

    Returns:
        np.int64 | np.ndarray: The number of business days between the start date and
        end dates. Returns a single integer if `end` is a single Timestamp, otherwise
        returns an ndarray of integers.

    Note:
        For more information on error handling, see numpy.busday_count documentation at
        https://numpy.org/doc/stable/reference/generated/numpy.busday_count.html.
        If a start date before 26/12/2023 is used, the old list of brazilian holidays is
        selected. Otherwise, the new list is be used.

    Examples:
        >>> import pandas as pd
        >>> start = '2023-12-15'
        >>> end = '2024-01-01'
        >>> count_bdays(start, end)
        10

        >>> start = '2023-01-01'
        >>> end = pd.Series('2023-01-31', '2023-03-01'])
        >>> count_bdays(start, end)
        array([22, 40])
    """
    # Convert to pandas Timestamp
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)

    # Convert to numpy data types
    start = start.to_numpy().astype("datetime64[D]")
    if isinstance(end, pd.Timestamp):
        end = pd.Series(end)
    end = end.values.astype("datetime64[D]")

    # Determine which list of holidays to use
    cutoff_date = np.datetime64("2023-12-26", "D")
    if start < cutoff_date:
        holiday_list = BR_HOLIDAYS_OLD
    else:
        holiday_list = BR_HOLIDAYS

    return np.busday_count(start, end, holidays=holiday_list)


def generate_bdays(start, end, holiday_list=BR_HOLIDAYS) -> pd.Series:
    """
    Generates a Series of business days between a `start` (inclusive) and
    `end` (inclusive) that takes into account the list of brazilian holidays as the
    default. This function is a wrapper for `pandas.bdate_range`.

    Args:
        start: str or datetime-like
        end: str or datetime-like

    Returns:
        pd.Series: A Series of business days between the start date and end date.

    Note:
        For more information on error handling, see pandas.bdate_range documentation at
        https://pandas.pydata.org/docs/reference/api/pandas.bdate_range.html#

    Examples:
        >>> import pandas as pd
        >>> start = '2023-12-20'
        >>> end = '2024-01-05'
        >>> generate_bdays(start, end)
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
    bdays = pd.bdate_range(start, end, freq="C", holidays=holiday_list)
    return pd.Series(bdays)
