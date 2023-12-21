from pathlib import Path
import pandas as pd
import numpy as np

holidays_path = Path(__file__).parent / "br_holidays.txt"
df = pd.read_csv(holidays_path, header=None, names=["date"], comment="#")
df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
# Using numpy array, speed is almost 10x faster
BR_HOLIDAYS = df["date"].values.astype("datetime64[D]")
# BR_HOLIDAYS = df["date"].dt.strftime("%Y-%m-%d").to_list()


def convert_to_np_datetime(dates):
    if isinstance(dates, pd.Timestamp):
        return dates.to_numpy().astype("datetime64[D]")
    elif isinstance(dates, pd.Series):
        return dates.values.astype("datetime64[D]")
    elif isinstance(dates, np.ndarray):
        return dates.astype("datetime64[D]")
    else:
        raise TypeError(
            f"Dates must be a Pandas Timestamp, Series, or Numpy ndarray. Received {type(dates)}."
        )


def count_business_days(
    start_dates: pd.Series | pd.Timestamp,
    end_dates: pd.Series | pd.Timestamp,
) -> np.int64 | np.ndarray:
    """
    Counts the number of business days between `start_dates` and `end_dates`, excluding
    the end date itself. If an end date is earlier than its corresponding start date, the
    count will be negative. This function is a wrapper for `numpy.busday_count` to be used
    directly with Pandas Timestamps and Series.

    Args:
        start_dates (pd.Series | pd.Timestamp): A Series or Timestamp representing the start dates.
        end_dates (pd.Series | pd.Timestamp): A Series or Timestamp representing the end dates.

    Returns:
        np.int64 | np.ndarray: The number of business days between start and end dates. Returns a
        single integer if `start_dates` and `end_dates` are Timestamps, otherwise returns an
        ndarray of integers.

    Note:
        For more information on error handling, see numpy.busday_count documentation at
        https://numpy.org/doc/stable/reference/generated/numpy.busday_count.html.

    Examples:
        >>> import pandas as pd
        >>> start = pd.Timestamp('2023-12-15')
        >>> end = pd.Timestamp('2024-01-01')
        >>> bday_count(start, end)
        10

        >>> start_series = pd.Series([pd.Timestamp('2023-01-01'), pd.Timestamp('2023-02-01')])
        >>> end_series = pd.Series([pd.Timestamp('2023-01-31'), pd.Timestamp('2023-02-28')])
        >>> bday_count(start_series, end_series)
        array([22, 18])
    """
    # Convert to numpy date format
    start_dates = convert_to_np_datetime(start_dates)
    end_dates = convert_to_np_datetime(end_dates)

    # Adjust start and end dates to the next business day if they fall on a holiday or weekend
    start_dates = np.busday_offset(start_dates, 0, roll="forward", holidays=BR_HOLIDAYS)
    end_dates = np.busday_offset(end_dates, 0, roll="forward", holidays=BR_HOLIDAYS)

    return np.busday_count(start_dates, end_dates, holidays=BR_HOLIDAYS)
