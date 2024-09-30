from typing import Literal, overload

import numpy as np
import pandas as pd

from pyield import date_converter as dc
from pyield.date_converter import ArrayDateTypes, ScalarDateTypes
from pyield.holidays import BrHolidays

type ArrayIntTypes = np.ndarray | pd.Series | list | tuple
type ScalarIntTypes = int | np.integer

# Initialize the BrHolidays class
br_holidays = BrHolidays()


@overload
def offset(
    dates: ScalarDateTypes,
    offset: ScalarIntTypes,
    roll: Literal["forward", "backward"] = ...,
    holiday_list: Literal["old", "new", "infer"] = ...,
) -> pd.Timestamp: ...


@overload
def offset(
    dates: ArrayDateTypes,
    offset: ArrayIntTypes,
    roll: Literal["forward", "backward"] = ...,
    holiday_list: Literal["old", "new", "infer"] = ...,
) -> pd.Series: ...


@overload
def offset(
    dates: ScalarDateTypes,
    offset: ArrayIntTypes,
    roll: Literal["forward", "backward"] = ...,
    holiday_list: Literal["old", "new", "infer"] = ...,
) -> pd.Series: ...


@overload
def offset(
    dates: ArrayDateTypes,
    offset: ScalarIntTypes,
    roll: Literal["forward", "backward"] = ...,
    holiday_list: Literal["old", "new", "infer"] = ...,
) -> pd.Series: ...


def offset(
    dates: ScalarDateTypes | ArrayDateTypes,
    offset: ScalarIntTypes | ArrayIntTypes,
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
        dates (ScalarDateTypes | ArrayDateTypes):
            The date(s) to offset. Can be a single date in various formats (string,
            `datetime`, `Timestamp`, etc.) or a collection of dates (list, tuple,
            `Series`, etc.).
        offset (int | Series | np.ndarray | list[int] | tuple[int], optional):
            The number of business days to offset the dates. Positive for
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
        >>> from pandas import Timestamp
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
        documentation: https://numpy.org/doc/stable/reference/generated/numpy.busday_offset.html
    """
    converted_dates = dc.convert_input_dates(dates)

    selected_holidays = br_holidays.get_applicable_holidays(
        converted_dates, holiday_list
    )
    selected_holidays_np = dc.convert_to_numpy_date(selected_holidays)

    dates_np = dc.convert_to_numpy_date(converted_dates)
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
    start: ScalarDateTypes,
    end: ScalarDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> int: ...


@overload
def count(
    start: ArrayDateTypes,
    end: ScalarDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


@overload
def count(
    start: ScalarDateTypes,
    end: ArrayDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


@overload
def count(
    start: ArrayDateTypes,
    end: ArrayDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series: ...


def count(
    start: ScalarDateTypes | ArrayDateTypes,
    end: ScalarDateTypes | ArrayDateTypes,
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> int | pd.Series:
    """
    Counts the number of business days between a `start` date (inclusive) and an `end`
    date (exclusive). The function can handle single dates, arrays of dates and
    mixed inputs, returning either a single integer or a series of integers depending
    on the inputs. It accounts for specified holidays, effectively excluding them from
    the business day count.

    Args:
        start (ScalarDateTypes | ArrayDateTypes): The start date(s)
            for counting.
        end (ScalarDateTypes | ArrayDateTypes): The end date(s) for counting, which
            is excluded from the count themselves.
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
    converted_start = dc.convert_input_dates(start)
    converted_end = dc.convert_input_dates(end)

    # Determine which list of holidays to use
    selected_holidays = br_holidays.get_applicable_holidays(
        converted_start, holiday_list
    )
    selected_holidays_np = dc.convert_to_numpy_date(selected_holidays)

    # Convert inputs to numpy datetime64[D] before calling numpy.busday_count
    start_np = dc.convert_to_numpy_date(converted_start)
    end_np = dc.convert_to_numpy_date(converted_end)

    result_np = np.busday_count(start_np, end_np, holidays=selected_holidays_np)
    if isinstance(result_np, np.integer):
        return int(result_np)
    else:
        return pd.Series(result_np, dtype="Int64")


def generate(
    start: ScalarDateTypes | None = None,
    end: ScalarDateTypes | None = None,
    inclusive: Literal["both", "neither", "left", "right"] = "both",
    holiday_list: Literal["old", "new", "infer"] = "infer",
) -> pd.Series:
    """
    Generates a Series of business days between a `start` and `end` date, considering
    the list of Brazilian holidays. It supports customization of holiday lists and
    inclusion options for start and end dates. It wraps `pandas.bdate_range`.

    Args:
        start (ScalarDateTypes | None, optional):
            The start date for generating business days. If None, the current date is
            used. Defaults to None.
        end (ScalarDateTypes | None, optional):
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
        converted_start = dc.convert_input_dates(start)
    else:
        converted_start = pd.Timestamp.today()

    if end:
        converted_end = dc.convert_input_dates(end)
    else:
        converted_end = pd.Timestamp.today()

    selected_holidays = br_holidays.get_applicable_holidays(
        converted_start, holiday_list
    )
    selected_holidays_list = selected_holidays.to_list()

    # Get the result as a DatetimeIndex (dti)
    result_dti = pd.bdate_range(
        start=converted_start,
        end=converted_end,
        freq="C",
        inclusive=inclusive,
        holidays=selected_holidays_list,
    )
    return pd.Series(result_dti.values)


def is_business_day(date: ScalarDateTypes) -> bool:
    """
    Checks if the input date is a business day.

    Args:
        date (ScalarDateTypes): The date to check.

    Returns:
        bool: True if the input date is a business day, False otherwise.

    Examples:
        >>> from pyield import bday
        >>> bday.is_business_day("25-12-2023")  # Christmas
        False
    """
    converted_date = dc.convert_input_dates(date)
    shifted_date = offset(converted_date, 0)  # Shift the date if it is not a bus. day
    return converted_date == shifted_date
