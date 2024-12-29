from typing import Literal, overload
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import pyield.date_converter as dc
import pyield.holidays as hl
from pyield.date_converter import DateArray, DateScalar

# Timezone for Brazil
TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")

IntegerScalar = int | np.integer
IntegerArray = np.ndarray | pd.Series | list | tuple


# Initialize Brazilian holidays data
br_holidays = hl.BrHolidays()
OLD_HOLIDAYS_ARRAY = br_holidays.get_holiday_array(holiday_option="old")
NEW_HOLIDAYS_ARRAY = br_holidays.get_holiday_array(holiday_option="new")


@overload
def offset(
    dates: DateScalar, offset: IntegerScalar, roll: Literal["forward", "backward"] = ...
) -> pd.Timestamp: ...


@overload
def offset(
    dates: DateArray, offset: IntegerArray, roll: Literal["forward", "backward"] = ...
) -> pd.Series: ...


@overload
def offset(
    dates: DateScalar, offset: IntegerArray, roll: Literal["forward", "backward"] = ...
) -> pd.Series: ...


@overload
def offset(
    dates: DateArray, offset: IntegerScalar, roll: Literal["forward", "backward"] = ...
) -> pd.Series: ...


def offset(
    dates: DateScalar | DateArray,
    offset: IntegerScalar | IntegerArray,
    roll: Literal["forward", "backward"] = "forward",
) -> pd.Timestamp | pd.Series:
    """
    First adjusts the date to fall on a valid day according to the roll rule, then
    applies offsets to the given dates to the next or previous business day, considering
    brazilian holidays. This function supports both single dates and collections of
    dates. It is a wrapper for `numpy.busday_offset` adapted for Pandas data types and
    brazilian holidays.

    **Important Note:** The `dates` parameter is used to determine which list of
    holidays applies to the calculation.

    Args:
        dates (DateScalar | DateArray): The date(s) to offset. Can be a scalar date type
            or a collection of dates. The holiday list is determined based on this date.
        offset (int | Series | np.ndarray | list[int] | tuple[int], optional):
            The number of business days to offset the dates. Positive for future dates,
            negative for past dates. Zero will return the same date if it's a business
            day, or the next business day otherwise.
        roll (Literal["forward", "backward"], optional): Direction to roll the date if
            it falls on a holiday or weekend. 'forward' to the next business day,
            'backward' to the previous. Defaults to 'forward'.

    Returns:
        pd.Timestamp | pd.Series: If a single date is provided, returns a single
            `Timestamp` of the offset date. If a series of dates is provided, returns a
            `Series` of offset dates.

    Examples:
        >>> from pyield import bday

        Offset to the next business day if not a bday (offset=0 and roll="forward")

        # Offset Saturday before Christmas to the next b. day (Tuesday after Christmas)
        >>> bday.offset("23-12-2023", 0)
        Timestamp('2023-12-26 00:00:00')

        # Offset Friday before Christmas (no offset because it's a business day)
        >>> bday.offset("22-12-2023", 0)
        Timestamp('2023-12-22 00:00:00')

        Offset to the previous business day if not a bday (offset=0 and roll="backward")

        # No offset because it's a business day
        >>> bday.offset("22-12-2023", 0, roll="backward")
        Timestamp('2023-12-22 00:00:00')

        # Offset to the first business day before "23-12-2023"
        >>> bday.offset("23-12-2023", 0, roll="backward")
        Timestamp('2023-12-22 00:00:00')

        Jump to the next business day (1 offset and roll="forward")

        # Offset Friday to the next business day (Friday is jumped -> Monday)
        >>> bday.offset("27-09-2024", 1)
        Timestamp('2024-09-30 00:00:00')

        # Offset Saturday to the next business day (Monday is jumped -> Tuesday)
        >>> bday.offset("28-09-2024", 1)
        Timestamp('2024-10-01 00:00:00')

        Jump to the previous business day (-1 offset and roll="backward")

        # Offset Friday to the previous business day (Friday is jumped -> Thursday)
        >>> bday.offset("27-09-2024", -1, roll="backward")
        Timestamp('2024-09-26 00:00:00')

        # Offset Saturday to the previous business day (Friday is jumped -> Thursday)
        >>> bday.offset("28-09-2024", -1, roll="backward")
        Timestamp('2024-09-26 00:00:00')

        List of dates and offsets

        >>> bday.offset(["19-09-2024", "20-09-2024"], 1)  # a list of dates
        0   2024-09-20
        1   2024-09-23
        dtype: datetime64[ns]

        >>> bday.offset("19-09-2024", [1, 2])  # a list of offsets
        0   2024-09-20
        1   2024-09-23
        dtype: datetime64[ns]

    Note:
        This function uses `numpy.busday_offset` under the hood, which means it follows
        the same conventions and limitations for business day calculations. For detailed
        information on error handling and behavior, refer to the `numpy.busday_offset`
        documentation:
        https://numpy.org/doc/stable/reference/generated/numpy.busday_offset.html
    """
    dates_pd = dc.convert_input_dates(dates)

    if isinstance(dates_pd, pd.Series):
        # Divide the input in order to apply the correct holiday list
        dates1 = dates_pd[dates_pd < br_holidays.TRANSITION_DATE]
        dates2 = dates_pd[dates_pd >= br_holidays.TRANSITION_DATE]
        dates3 = dates_pd[dates_pd.isna()]

        offsetted_dates1 = np.busday_offset(
            dc.to_numpy_date_type(dates1),
            offsets=offset,
            roll=roll,
            holidays=OLD_HOLIDAYS_ARRAY,
        )

        offsetted_dates2 = np.busday_offset(
            dc.to_numpy_date_type(dates2),
            offsets=offset,
            roll=roll,
            holidays=NEW_HOLIDAYS_ARRAY,
        )

        # Convert from numpy.datetime64 to pandas.Timestamp
        offsetted_dates1 = pd.to_datetime(offsetted_dates1)
        offsetted_dates2 = pd.to_datetime(offsetted_dates2)

        # 'pd.to_datetime' does not necessarily return datetime64[ns] Series
        offsetted_dates1 = pd.Series(offsetted_dates1).astype("datetime64[ns]")
        offsetted_dates2 = pd.Series(offsetted_dates2).astype("datetime64[ns]")

        # Use old index to rejoin the results
        offsetted_dates1.index = dates1.index
        offsetted_dates2.index = dates2.index

        offsetted_dates = pd.concat([offsetted_dates1, offsetted_dates2, dates3])

        # Reorder the result to match the original input order
        return offsetted_dates.sort_index()

    elif isinstance(dates_pd, pd.Timestamp):
        offsetted_dates_np = np.busday_offset(
            dc.to_numpy_date_type(dates_pd),
            offsets=offset,
            roll=roll,
            holidays=br_holidays.get_holiday_array(dates_pd),
        )
        if isinstance(offsetted_dates_np, np.datetime64):
            return pd.Timestamp(offsetted_dates_np).as_unit("ns")
        else:
            return pd.Series(offsetted_dates_np).astype("datetime64[ns]")

    else:
        raise ValueError("Invalid input type for 'dates'.")


@overload
def count(start: DateScalar, end: DateScalar) -> int: ...


@overload
def count(start: DateArray, end: DateScalar) -> pd.Series: ...


@overload
def count(start: DateScalar, end: DateArray) -> pd.Series: ...


@overload
def count(start: DateArray, end: DateArray) -> pd.Series: ...


def count(
    start: DateScalar | DateArray,
    end: DateScalar | DateArray,
) -> int | pd.Series:
    """
    Counts the number of business days between a `start` date (inclusive) and an `end`
    date (exclusive). The function can handle single dates, arrays of dates and
    mixed inputs, returning either a single integer or a series of integers depending
    on the inputs. It accounts for specified holidays, effectively excluding them from
    the business day count.

    **Important Note:** The `start` date is used to determine which list of holidays
    applies to the calculation.

    Args:
        start (DateScalar | DateArray): The start date(s) for counting. The holiday list
            is selected based on this date.
        end (DateScalar | DateArray): The end date(s) for counting, which
            is excluded from the count themselves.

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
        - The `start` date determines the holiday list, ensuring consistency with the
          applicable calendar at the time.
        - See `numpy.busday_count` documentation for more details on how holidays are
          handled and how business day counts are calculated:
          https://numpy.org/doc/stable/reference/generated/numpy.busday_count.html.

    Examples:
        >>> from pyield import bday

        >>> bday.count("15-12-2023", "01-01-2024")
        10

        # Total business days in January and February since the start of the year
        >>> bday.count(start="01-01-2024", end=["01-02-2024", "01-03-2024"])
        0    22
        1    41
        dtype: Int64

        # The remaining business days in January and February to the end of the year
        >>> bday.count(["01-01-2024", "01-02-2024"], "01-01-2025")
        0    253
        1    231
        dtype: Int64

        # The total business days in January and February of 2024
        >>> bday.count(["01-01-2024", "01-02-2024"], ["01-02-2024", "01-03-2024"])
        0    22
        1    19
        dtype: Int64
    """
    start_pd = dc.convert_input_dates(start)
    end_pd = dc.convert_input_dates(end)

    # If inputs are Series, check if they have different lengths
    if isinstance(start_pd, pd.Series) and isinstance(end_pd, pd.Series):
        if start_pd.size != end_pd.size:
            raise ValueError("Input Series must have the same length.")

    # Only start is used to determine the holiday list
    if isinstance(start_pd, pd.Series):
        # Divide the input in order to apply the correct holiday list
        start1 = start_pd[start_pd < br_holidays.TRANSITION_DATE]
        start2 = start_pd[start_pd >= br_holidays.TRANSITION_DATE]
        start3 = start_pd[start_pd.isna()]

        # If end is a Series, it must be divided as well
        if isinstance(end_pd, pd.Series):
            end1 = end_pd[start1.index]
            end2 = end_pd[start2.index]
        else:
            end1 = end_pd
            end2 = end_pd

        result1 = np.busday_count(
            begindates=dc.to_numpy_date_type(start1),
            enddates=dc.to_numpy_date_type(end1),
            holidays=OLD_HOLIDAYS_ARRAY,
        )
        result2 = np.busday_count(
            begindates=dc.to_numpy_date_type(start2),
            enddates=dc.to_numpy_date_type(end2),
            holidays=NEW_HOLIDAYS_ARRAY,
        )

        # Prepare results to be rejoined
        result1 = pd.Series(result1, dtype="Int64")
        result2 = pd.Series(result2, dtype="Int64")
        # Convert the third result from NaT to NA
        result3 = pd.Series(pd.NA, index=start3.index, dtype="Int64")

        # Old index is used to rejoin the results
        result1.index = start1.index
        result2.index = start2.index

        # Reorder the result to match the original input order
        result = pd.concat([result1, result2, result3]).sort_index()

    else:  # Start is a single date
        result = np.busday_count(
            begindates=dc.to_numpy_date_type(start_pd),
            enddates=dc.to_numpy_date_type(end_pd),
            holidays=br_holidays.get_holiday_array(start_pd),
        )
        result = pd.Series(result, dtype="Int64")

    if result.size == 1:
        return int(result[0])
    return result


def generate(
    start: DateScalar | None = None,
    end: DateScalar | None = None,
    inclusive: Literal["both", "neither", "left", "right"] = "both",
    holiday_option: Literal["old", "new", "infer"] = "infer",
) -> pd.Series:
    """
    Generates a Series of business days between a `start` and `end` date, considering
    the list of Brazilian holidays. It supports customization of holiday lists and
    inclusion options for start and end dates. It wraps `pandas.bdate_range`.

    Args:
        start (DateScalar | None, optional): The start date for generating the dates.
             If None, the current date is used. Defaults to None.
        end (DateScalar | None, optional): The end date for generating business days.
            If None, the current date is used. Defaults to None.
        inclusive (Literal["both", "neither", "left", "right"], optional):
            Determines which of the start and end dates are included in the result.
            Valid options are 'both', 'neither', 'left', 'right'. Defaults to 'both'.
        holiday_option (Literal["old", "new", "infer"], optional):
            Specifies the list of holidays to consider. 'old' or 'new' refer to
            predefined lists, 'infer' selects the list based on the most recent date in
            the range. Defaults to "infer".

    Returns:
        pd.Series: A Series representing a range of business days between the specified
            start and end dates, considering the specified holidays.

    Examples:
        >>> from pyield import bday
        >>> bday.generate(start="22-12-2023", end="02-01-2024")
        0   2023-12-22
        1   2023-12-26
        2   2023-12-27
        3   2023-12-28
        4   2023-12-29
        5   2024-01-02
        dtype: datetime64[ns]

    Note:
        For detailed information on parameters and error handling, refer to
        `pandas.bdate_range` documentation:
        https://pandas.pydata.org/docs/reference/api/pandas.bdate_range.html.
    """
    if start:
        start_pd = dc.convert_input_dates(start)
    else:
        start_pd = pd.Timestamp.today()

    if end:
        end_pd = dc.convert_input_dates(end)
    else:
        end_pd = pd.Timestamp.today()

    holidays_list = br_holidays.get_holiday_series(
        dates=start_pd, holiday_option=holiday_option
    ).to_list()

    # Get the result as a DatetimeIndex (dti)
    result_dti = pd.bdate_range(
        start=start_pd,
        end=end_pd,
        freq="C",
        inclusive=inclusive,
        holidays=holidays_list,
    )
    return pd.Series(result_dti.values)


def is_business_day(date: DateScalar) -> bool:
    """
    Checks if the input date is a business day.

    Args:
        date (DateScalar): The date to check.

    Returns:
        bool: True if the input date is a business day, False otherwise.

    Examples:
        >>> from pyield import bday
        >>> bday.is_business_day("25-12-2023")  # Christmas
        False
    """
    date_pd = dc.convert_input_dates(date)
    shifted_date = offset(date_pd, 0)  # Shift the date if it is not a bus. day
    return date_pd == shifted_date


def last_business_day() -> pd.Timestamp:
    """
    Returns the last business day in Brazil. If the current date is a business day, it
    returns the current date. If it is a weekend or holiday, it returns the last
    business day before the current date.

    Returns:
        pd.Timestamp: The last business day in Brazil.

    """
    # Get the current date in Brazil without timezone information
    bz_today = pd.Timestamp.now(TIMEZONE_BZ).normalize().tz_localize(None)
    return offset(bz_today, 0, roll="backward")
