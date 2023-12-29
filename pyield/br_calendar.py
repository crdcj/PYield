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
    date: pd.Timestamp, holiday_list: np.ndarray = BR_HOLIDAYS
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
        >>> date = pd.Timestamp('2023-12-23') # Saturday before Christmas
        >>> adjust_to_next_business_day(date)
        Timestamp('2023-12-26')
    """
    # Convert to numpy data type
    date = date.to_numpy().astype("datetime64[D]")

    # Adjust to next business day
    adj_date = np.busday_offset(date, 0, roll="forward", holidays=holiday_list)

    # Convert back to pandas data type
    return pd.Timestamp(adj_date)


def count_business_days(
    start_date: pd.Timestamp,
    end_dates: pd.Series | pd.Timestamp,
) -> np.int64 | np.ndarray:
    """
    Counts the number of business days between a `start_date` (inclusive) and
    `end_dates` (exclusive). If an end date is earlier than the start date, the count
    will be negative. This function is a wrapper for `numpy.busday_count` to be used
    directly with Pandas data types. The start date is used to determine which list of
    holidays to use. Because of this, a single value for start date is necessary in order
    to use the numpy function with the right list of holidays.

    Args:
        start_date (pd.Timestamp): A Timestamp representing the start date.
        end_dates (pd.Series | pd.Timestamp): A Series or Timestamp representing the end dates.

    Returns:
        np.int64 | np.ndarray: The number of business days between the start date and
        end dates. Returns a single integer if `end_dates` is a single Timestamp, otherwise
        returns an ndarray of integers.

    Note:
        For more information on error handling, see numpy.busday_count documentation at
        https://numpy.org/doc/stable/reference/generated/numpy.busday_count.html.
        If a start date before 26/12/2023 is used, the old list of brazilian holidays is
        selected. Otherwise, the new list is be used.

    Examples:
        >>> import pandas as pd
        >>> start = pd.Timestamp('2023-12-15')
        >>> end = pd.Timestamp('2024-01-01')
        >>> count_business_days(start, end)
        10

        >>> start = pd.Timestamp('2023-01-01')
        >>> end = pd.Series([pd.Timestamp('2023-01-31'), pd.Timestamp('2023-03-01')])
        >>> count_business_days(start, end)
        array([22, 40])
    """
    # Convert to numpy data types
    start_date = start_date.to_numpy().astype("datetime64[D]")
    if isinstance(end_dates, pd.Timestamp):
        end_dates = pd.Series(end_dates)
    end_dates = end_dates.values.astype("datetime64[D]")

    # Determine which list of holidays to use
    cutoff_date = np.datetime64("2023-12-26", "D")
    if start_date < cutoff_date:
        holiday_list = BR_HOLIDAYS_OLD
    else:
        holiday_list = BR_HOLIDAYS

    return np.busday_count(start_date, end_dates, holidays=holiday_list)
