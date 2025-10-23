import datetime as dt
from typing import Literal, overload

import pandas as pd
import polars as pl

import pyield.bday.holidays as hl
import pyield.converters as cv
from pyield.config import TIMEZONE_BZ
from pyield.types import (
    DateArray,
    DateScalar,
    IntegerArray,
    IntegerScalar,
    has_null_args,
)

# Initialize Brazilian holidays data
br_holidays = hl.BrHolidays()
OLD_HOLIDAYS_ARRAY = br_holidays.get_holiday_series(holiday_option="old")
NEW_HOLIDAYS_ARRAY = br_holidays.get_holiday_series(holiday_option="new")
TRANSITION_DATE = br_holidays.TRANSITION_DATE
_WEEKEND_START = 5  # Python weekday() >= 5 means Saturday/Sunday


@overload
def count(start: DateScalar, end: DateScalar) -> int | None: ...
@overload
def count(start: DateArray, end: DateScalar | DateArray) -> pl.Series: ...
@overload
def count(start: DateScalar, end: DateArray) -> pl.Series: ...
def count(
    start: DateScalar | DateArray,
    end: DateScalar | DateArray,
) -> int | pl.Series | None:
    """
    Count business days between `start` (inclusive) and `end` (exclusive) with
    Brazilian holiday adjustment and per-row holiday regime selection.

    ORDER PRESERVATION (critical): The output order ALWAYS matches the element-wise
    order of the original inputs. No sorting, deduplication, alignment or reshaping is
    performed. If you pass arrays, the i-th result corresponds to the i-th pair of
    (`start`, `end`) after broadcasting. This guarantees safe assignment back to the
    originating DataFrame.

    Holiday regime: For each `start` value, the holiday list (old vs. new) is chosen
    based on the transition date 2023-12-26 (`TRANSITION_DATE`). Starts before the
    transition use the old list for that row's count; starts on/after use the new list.

    Null propagation: If any scalar argument is null, returns `None`. Nulls inside
    array inputs yield nulls in corresponding result positions.

    Return type: If both inputs are scalars (non-null) an `int` is returned; otherwise
    a `polars.Series` of int counts (name: 'bdays'). If a null scalar short-circuits,
    `None` is returned.

    Args:
        start: Single date or collection (inclusive boundary).
        end: Single date or collection (exclusive boundary).

    Returns:
        int | pl.Series | None: Returns an integer or None if `start` and `end` are
            single dates, or a Series if any of them is an array of dates.

    Notes:
        - This function is a wrapper around `polars.business_day_count`.
        - The holiday list is determined per-row based on the `start` date.

    Examples:
        >>> from pyield import bday
        >>> bday.count("15-12-2023", "01-01-2024")
        10

        Total business days in January and February since the start of the year
        >>> bday.count(start="01-01-2024", end=["01-02-2024", "01-03-2024"])
        shape: (2,)
        Series: 'bdays' [i64]
        [
            22
            41
        ]

        The remaining business days from January/February until the end of the year
        >>> bday.count(["01-01-2024", "01-02-2024"], "01-01-2025")
        shape: (2,)
        Series: 'bdays' [i64]
        [
            253
            231
        ]

        The total business days in January and February of 2024
        >>> bday.count(["01-01-2024", "01-02-2024"], ["01-02-2024", "01-03-2024"])
        shape: (2,)
        Series: 'bdays' [i64]
        [
            22
            19
        ]

        Null values are propagated
        >>> bday.count(None, "01-01-2024")  # None start

        >>> bday.count("01-01-2024", None)  # None end

        >>> bday.count("01-01-2024", ["01-02-2024", None])  # None in end array
        shape: (2,)
        Series: 'bdays' [i64]
        [
            22
            null
        ]

        >>> start_dates = ["01-01-2024", "01-02-2024", "01-03-2024"]
        >>> bday.count(start_dates, "01-01-2025")
        shape: (3,)
        Series: 'bdays' [i64]
        [
            253
            231
            212
        ]
    """
    # Validate and normalize inputs
    if has_null_args(start, end):
        return None
    start_pl = cv.convert_dates(start)
    end_pl = cv.convert_dates(end)

    # Coloca as séries em um DataFrame para trabalhar com expressões em colunas
    df = pl.DataFrame(
        {"start": start_pl, "end": end_pl},
        schema={"start": pl.Date, "end": pl.Date},
        nan_to_null=True,
    )

    result_expr = (
        pl.when(pl.col("start") < TRANSITION_DATE)
        .then(
            pl.business_day_count(
                start=pl.col("start"), end=pl.col("end"), holidays=OLD_HOLIDAYS_ARRAY
            ),
        )
        .otherwise(
            pl.business_day_count(
                start=pl.col("start"), end=pl.col("end"), holidays=NEW_HOLIDAYS_ARRAY
            )
        )
        .cast(pl.Int64)
    )

    s_bdays = df.select(result_expr.alias("bdays"))["bdays"]

    # Se a entrada original era escalar, retorna o valor escalar
    if len(s_bdays) == 1:
        return s_bdays.item()

    return s_bdays


@overload
def offset(
    dates: DateScalar, offset: IntegerScalar, roll: Literal["forward", "backward"] = ...
) -> dt.date | None: ...
@overload
def offset(
    dates: DateArray,
    offset: IntegerArray | IntegerScalar,
    roll: Literal["forward", "backward"] = ...,
) -> pl.Series: ...
@overload
def offset(
    dates: DateScalar, offset: IntegerArray, roll: Literal["forward", "backward"] = ...
) -> pl.Series: ...
def offset(
    dates: DateScalar | DateArray,
    offset: IntegerScalar | IntegerArray,
    roll: Literal["forward", "backward"] = "forward",
) -> dt.date | pl.Series | None:
    """
    Offset date(s) by a number of business days with per-row Brazilian holiday
    regime selection. The operation is performed in two steps per element:
    1) ROLL: If the original date falls on a weekend or holiday, move it according
       to ``roll`` ("forward" -> next business day; "backward" -> previous).
    2) ADD: Apply the signed business-day ``offset`` (positive forward, negative
       backward, zero = stay on the rolled date).

    ORDER PRESERVATION (critical): Output ordering strictly matches the element-wise
    pairing after broadcasting between ``dates`` and ``offset``. No sorting,
    deduplication or shape changes occur. The i-th result corresponds to the i-th
    (date, offset) pair, enabling safe assignment back into the originating DataFrame.

    Holiday regime: For EACH date the appropriate holiday list (old vs. new) is
    chosen based on the transition date ``2023-12-26`` (``TRANSITION_DATE``). Dates
    before the transition use the *old* list; dates on/after use the *new* list.

    Roll semantics: ``roll`` only acts when the original date is not already a
    business day under its regime. After rolling, the subsequent business-day
    addition is applied from that rolled anchor. An ``offset`` of 0 therefore
    returns either the original date (if already a business day) or the rolled
    business day.

    Null propagation: If any scalar argument is null, the function short-circuits
    to ``None``. Nulls inside array inputs propagate to their corresponding output
    positions.

    Broadcasting: ``dates`` and ``offset`` may be scalars or array-like. Standard
    Polars broadcasting rules apply when constructing the per-row pairs.

    Return type: If both inputs are non-null scalars a ``datetime.date`` is returned.
    Otherwise a ``polars.Series`` of dates named ``'result'`` is produced. Null scalar
    inputs yield ``None``.

    Args:
        dates: Single date or collection of dates to be rolled (if needed) and then
            offset. Each date independently selects the holiday regime.
        offset: Signed count of business days to apply after rolling. Positive moves
            forward, negative backward, zero keeps the rolled anchor.
        roll: Direction to roll a non-business starting date ("forward" or
            "backward"). Defaults to "forward".

    Returns:
        dt.date | pl.Series | None: A Python ``date`` for scalar inputs, a Polars
        Series of dates for any array input, or ``None`` if a null scalar argument
        was provided.

    Notes:
        - Wrapper around ``polars.Expr.dt.add_business_days`` applied conditionally.
        - Holiday regime is decided per element by comparing to ``TRANSITION_DATE``.
        - Weekends are always treated as non-business days.

    Examples:
        >>> from pyield import bday

        Offset Saturday before Christmas to the next b. day (Tuesday after Christmas)
        >>> bday.offset("23-12-2023", 0)
        datetime.date(2023, 12, 26)

        Offset Friday before Christmas (no offset because it's a business day)
        >>> bday.offset("22-12-2023", 0)
        datetime.date(2023, 12, 22)

        Offset to the previous business day if not a bday (offset=0 and roll="backward")

        No offset because it's a business day
        >>> bday.offset("22-12-2023", 0, roll="backward")
        datetime.date(2023, 12, 22)

        Offset to the first business day before "23-12-2023"
        >>> bday.offset("23-12-2023", 0, roll="backward")
        datetime.date(2023, 12, 22)

        Jump to the next business day (1 offset and roll="forward")

        Offset Friday to the next business day (Friday is jumped -> Monday)
        >>> bday.offset("27-09-2024", 1)
        datetime.date(2024, 9, 30)

        Offset Saturday to the next business day (Monday is jumped -> Tuesday)
        >>> bday.offset("28-09-2024", 1)
        datetime.date(2024, 10, 1)

        Jump to the previous business day (-1 offset and roll="backward")

        Offset Friday to the previous business day (Friday is jumped -> Thursday)
        >>> bday.offset("27-09-2024", -1, roll="backward")
        datetime.date(2024, 9, 26)

        Offset Saturday to the previous business day (Friday is jumped -> Thursday)
        >>> bday.offset("28-09-2024", -1, roll="backward")
        datetime.date(2024, 9, 26)

        # List of dates and offsets
        >>> bday.offset(["19-09-2024", "20-09-2024"], 1)
        shape: (2,)
        Series: 'result' [date]
        [
            2024-09-20
            2024-09-23
        ]

        >>> bday.offset("19-09-2024", [1, 2])  # a list of offsets
        shape: (2,)
        Series: 'result' [date]
        [
            2024-09-20
            2024-09-23
        ]

        # Null values are propagated
        >>> print(bday.offset(None, 1))
        None

        >>> print(bday.offset(None, [1, 2]))
        None

        >>> bday.offset(["19-09-2024", None], 1)
        shape: (2,)
        Series: 'result' [date]
        [
            2024-09-20
            null
        ]

        >>> dates = ["19-09-2024", "20-09-2024", "21-09-2024"]
        >>> bday.offset(dates, 1)
        shape: (3,)
        Series: 'result' [date]
        [
            2024-09-20
            2024-09-23
            2024-09-24
        ]

    Note:
        This function uses `polars.Expr.dt.add_business_days` under the hood. For
        detailed information, refer to the Polars documentation.
    """
    # Validate and normalize inputs
    if has_null_args(dates, offset):
        return None
    dates_pl = cv.convert_dates(dates)

    # Coloca as entradas em um DataFrame para trabalhar com expressões em colunas
    df = pl.DataFrame(
        {"dates": dates_pl, "offset": offset},
        schema={"dates": pl.Date, "offset": pl.Int64},
        nan_to_null=True,
    )

    # Cria a expressão condicional para aplicar a lista de feriados correta
    result_expr = (
        pl.when(pl.col("dates") < TRANSITION_DATE)
        .then(
            pl.col("dates").dt.add_business_days(
                n=pl.col("offset"),
                roll=roll,
                holidays=OLD_HOLIDAYS_ARRAY,
            )
        )
        .otherwise(
            pl.col("dates").dt.add_business_days(
                n=pl.col("offset"),
                roll=roll,
                holidays=NEW_HOLIDAYS_ARRAY,
            )
        )
    )

    # Executa a expressão e obtém a série de resultados
    result_series = df.select(result_expr.alias("result"))["result"]

    # Se a entrada original era escalar, retorna o valor escalar
    if len(result_series) == 1:
        return result_series.item()

    return result_series


def generate(
    start: DateScalar | None = None,
    end: DateScalar | None = None,
    inclusive: Literal["both", "neither", "left", "right"] = "both",
    holiday_option: Literal["old", "new", "infer"] = "new",
) -> pl.Series:
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
            Specifies the list of holidays to consider. Defaults to "new".
            - **'old'**: Uses the holiday list effective *before* the transition date
            of 2023-12-26.
            - **'new'**: Uses the holiday list effective *on and after* the transition
            date of 2023-12-26.
            - **'infer'**: Automatically selects the holiday list ('old' or 'new') based
            on the `start` date relative to the transition date (2023-12-26). If `start`
            is before the transition, 'old' is used; otherwise, 'new' is used.

    Returns:
        pl.Series: A Series representing a range of business days between the specified
            start and end dates, considering the specified holidays.

    Examples:
        >>> from pyield import bday
        >>> bday.generate(start="22-12-2023", end="02-01-2024")
        shape: (6,)
        Series: '' [date]
        [
            2023-12-22
            2023-12-26
            2023-12-27
            2023-12-28
            2023-12-29
            2024-01-02
        ]

    Note:
        For detailed information on parameters and error handling, refer to
        `pandas.bdate_range` documentation:
        https://pandas.pydata.org/docs/reference/api/pandas.bdate_range.html.
    """
    if start:
        start_pd = cv.convert_dates(start)
    else:
        start_pd = dt.datetime.now(TIMEZONE_BZ).date()

    if end:
        end_pd = cv.convert_dates(end)
    else:
        end_pd = dt.datetime.now(TIMEZONE_BZ).date()

    applicable_holidays = br_holidays.get_holiday_series(
        dates=start_pd, holiday_option=holiday_option
    ).to_list()

    # Get the result as a DatetimeIndex (dti)
    result_dti = pd.bdate_range(
        start=start_pd,
        end=end_pd,
        freq="C",
        inclusive=inclusive,
        holidays=applicable_holidays,
    )
    s_pd = pd.Series(result_dti.values).astype("date32[pyarrow]")
    return pl.Series(s_pd)


@overload
def is_business_day(dates: DateScalar) -> bool | None: ...
@overload
def is_business_day(dates: DateArray) -> pl.Series: ...
def is_business_day(dates: DateScalar | DateArray) -> bool | pl.Series | None:
    """Check if date(s) are business day(s) in Brazil.

    This function applies the correct holiday list (old vs. new) for EACH date
    relative to the transition date ``2023-12-26``. Dates before the transition
    use the *old* holiday list; dates on/after the transition use the *new*
    holiday list.

    Behavior mirrors other functions in this module: if the resulting length is 1
    (even if the user passed a single-element collection) a Python ``bool`` (or
    ``None`` for null input) is returned; otherwise a ``polars.Series`` of booleans
    is returned with nulls propagated.

    Args:
        dates: A single date or a collection of dates (scalar, list/tuple/Series,
            numpy array, Polars/Pandas Series). Null scalar inputs return ``None``.

    Returns:
        bool | pl.Series | None: ``True`` if business day, ``False`` if not, ``None``
        for null scalar input, or a Polars boolean Series for array inputs.

    Examples:
        >>> from pyield import bday
        >>> bday.is_business_day("25-12-2023")  # Christmas (old calendar)
        False
        >>> bday.is_business_day("20-11-2024")  # National Zumbi Day (new holiday)
        False
        >>> bday.is_business_day(["22-12-2023", "26-12-2023"])  # Mixed periods
        shape: (2,)
        Series: 'is_bday' [bool]
        [
            true
            true
        ]

    Notes:
        - The transition date is defined in ``TRANSITION_DATE``.
        - Null elements in array inputs propagate as nulls.
        - Weekends are never business days.
    """
    # Validate and normalize inputs
    if has_null_args(dates):
        return None
    converted = cv.convert_dates(dates)

    # Build DataFrame to allow conditional expression selecting the right holiday list
    df = pl.DataFrame({"dates": converted}, schema={"dates": pl.Date}, nan_to_null=True)

    result_expr = (
        pl.when(pl.col("dates") < TRANSITION_DATE)
        .then(
            pl.col("dates").dt.is_business_day(holidays=OLD_HOLIDAYS_ARRAY),
        )
        .otherwise(
            pl.col("dates").dt.is_business_day(holidays=NEW_HOLIDAYS_ARRAY),
        )
    )

    s_result = df.select(result_expr.alias("is_bday"))["is_bday"]

    if len(s_result) == 1:
        return s_result.item()

    return s_result


def last_business_day() -> dt.date:
    """
    Returns the last business day in Brazil. If the current date is a business day, it
    returns the current date. If it is a weekend or holiday, it returns the last
    business day before the current date.

    Returns:
        dt.date: The last business day in Brazil.

    Notes:
        - The determination of the last business day considers the correct Brazilian
        holiday list (before or after the 2023-12-26 transition) applicable to
        the current date.

    """
    # Get the current date in Brazil without timezone information
    bz_today = dt.datetime.now(TIMEZONE_BZ).date()
    result = offset(bz_today, 0, roll="backward")
    assert isinstance(result, dt.date), (
        "Assumption violated: offset did not return a date for the current date."
    )
    return result
