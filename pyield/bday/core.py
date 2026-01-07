import datetime as dt
from typing import Literal, overload

import pandas as pd
import polars as pl

import pyield.bday.holidays as hl
import pyield.converters as cv
import pyield.types as tp
from pyield import clock
from pyield.types import ArrayLike, DateLike

# Initialize Brazilian holidays class
br_holidays = hl.BrHolidays()
OLD_HOLIDAYS_ARRAY = br_holidays.get_holiday_series(holiday_option="old")
NEW_HOLIDAYS_ARRAY = br_holidays.get_holiday_series(holiday_option="new")
TRANSITION_DATE = br_holidays.TRANSITION_DATE


@overload
def count(start: ArrayLike, end: ArrayLike | DateLike | None) -> pl.Series: ...
@overload
def count(start: DateLike | None, end: ArrayLike) -> pl.Series: ...
@overload
def count(start: DateLike, end: DateLike) -> int: ...
@overload
def count(start: DateLike, end: None) -> None: ...
@overload
def count(start: None, end: DateLike | None) -> None: ...


def count(
    start: None | DateLike | ArrayLike,
    end: None | DateLike | ArrayLike,
) -> None | int | pl.Series:
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
    a `polars.Series` of int counts (name: 'bday_count').
    If a null scalar short-circuits, `None` is returned.

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
        Series: 'bday_count' [i64]
        [
            22
            41
        ]

        The remaining business days from January/February until the end of the year
        >>> bday.count(["01-01-2024", "01-02-2024"], "01-01-2025")
        shape: (2,)
        Series: 'bday_count' [i64]
        [
            253
            231
        ]

        The total business days in January and February of 2024
        >>> bday.count(["01-01-2024", "01-02-2024"], ["01-02-2024", "01-03-2024"])
        shape: (2,)
        Series: 'bday_count' [i64]
        [
            22
            19
        ]

        Null values are propagated
        >>> bday.count(None, "01-01-2024")  # None start

        >>> bday.count("01-01-2024", None)  # None end

        >>> bday.count("01-01-2024", ["01-02-2024", None])  # None in end array
        shape: (2,)
        Series: 'bday_count' [i64]
        [
            22
            null
        ]

        >>> start_dates = ["01-01-2024", "01-02-2024", "01-03-2024"]
        >>> bday.count(start_dates, "01-01-2025")
        shape: (3,)
        Series: 'bday_count' [i64]
        [
            253
            231
            212
        ]
    """
    # Coloca as séries em um DataFrame para trabalhar com expressões em colunas
    df = pl.DataFrame(
        data={"start": cv.convert_dates(start), "end": cv.convert_dates(end)},
        schema={"start": pl.Date, "end": pl.Date},
        nan_to_null=True,
    )

    count_expr = (
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
        .alias("bday_count")
    )

    s = df.select(count_expr)["bday_count"]

    if not tp.has_array_like_args(start, end):
        return s.first()

    return s


@overload
def offset(
    dates: ArrayLike,
    offset: ArrayLike | int | None,
    roll: Literal["forward", "backward"] = ...,
) -> pl.Series: ...
@overload
def offset(
    dates: DateLike | None,
    offset: ArrayLike,
    roll: Literal["forward", "backward"] = ...,
) -> pl.Series: ...
@overload
def offset(
    dates: DateLike,
    offset: int,
    roll: Literal["forward", "backward"] = ...,
) -> dt.date: ...
@overload
def offset(
    dates: None,
    offset: int,
    roll: Literal["forward", "backward"] = ...,
) -> None: ...
@overload
def offset(
    dates: DateLike,
    offset: None,
    roll: Literal["forward", "backward"] = ...,
) -> None: ...


def offset(
    dates: DateLike | ArrayLike | None,
    offset: int | ArrayLike | None,
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
    Otherwise a ``polars.Series`` of dates named ``'adjusted_date'`` is produced.
    Null scalar inputs yield ``None``.

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
        Series: 'adjusted_date' [date]
        [
            2024-09-20
            2024-09-23
        ]

        >>> bday.offset("19-09-2024", [1, 2])  # a list of offsets
        shape: (2,)
        Series: 'adjusted_date' [date]
        [
            2024-09-20
            2024-09-23
        ]

        # Scalar nulls propagate to None
        >>> print(bday.offset(None, 1))
        None

        # Scalar null propagates inside arrays
        >>> bday.offset(None, [1, 2])
        shape: (2,)
        Series: 'adjusted_date' [date]
        [
            null
            null
        ]

        # Nulls inside arrays are preserved
        >>> bday.offset(["19-09-2024", None], 1)
        shape: (2,)
        Series: 'adjusted_date' [date]
        [
            2024-09-20
            null
        ]

        >>> dates = ["19-09-2024", "20-09-2024", "21-09-2024"]
        >>> bday.offset(dates, 1)
        shape: (3,)
        Series: 'adjusted_date' [date]
        [
            2024-09-20
            2024-09-23
            2024-09-24
        ]

    Note:
        This function uses `polars.Expr.dt.add_business_days` under the hood. For
        detailed information, refer to the Polars documentation.
    """
    # Coloca as entradas em um DataFrame para trabalhar com expressões em colunas
    df = pl.DataFrame(
        data={"dates": cv.convert_dates(dates), "offset": offset},
        schema={"dates": pl.Date, "offset": pl.Int64},
        nan_to_null=True,
    )

    # Cria a expressão condicional para aplicar a lista de feriados correta
    offset_expr = (
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
        .alias("adjusted_date")
    )

    # Executa a expressão e obtém a série de resultados
    s = df.select(offset_expr)["adjusted_date"]

    if not tp.has_array_like_args(dates, offset):
        return s.first()

    return s


def generate(
    start: DateLike | None = None,
    end: DateLike | None = None,
    inclusive: Literal["both", "neither", "left", "right"] = "both",
    holiday_option: Literal["old", "new", "infer"] = "new",
) -> pl.Series:
    """
    Generates a Series of business days between a `start` and `end` date, considering
    the list of Brazilian holidays. It supports customization of holiday lists and
    inclusion options for start and end dates. It wraps `pandas.bdate_range`.

    Args:
        start (DateLike | None, optional): The start date for generating the dates.
             If None, the current date is used. Defaults to None.
        end (DateLike | None, optional): The end date for generating business days.
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
        Series: 'bday' [date]
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
    conv_start = cv.convert_dates(start)
    today = clock.today()
    if not conv_start:
        conv_start = today

    conv_end = cv.convert_dates(end)
    if not conv_end:
        conv_end = today

    applicable_holidays = br_holidays.get_holiday_series(
        dates=conv_start, holiday_option=holiday_option
    ).to_list()

    # Get the result as a DatetimeIndex (dti)
    result_dti = pd.bdate_range(
        start=conv_start,
        end=conv_end,
        freq="C",
        inclusive=inclusive,
        holidays=applicable_holidays,
    )
    s_pd = pd.Series(result_dti.values, name="bday").astype("date32[pyarrow]")
    return pl.from_pandas(s_pd)


@overload
def is_business_day(dates: None) -> None: ...
@overload
def is_business_day(dates: DateLike) -> bool: ...
@overload
def is_business_day(dates: ArrayLike) -> pl.Series: ...


def is_business_day(dates: None | DateLike | ArrayLike) -> None | bool | pl.Series:
    """Determine whether date(s) are Brazilian business days with per-element
    holiday regime selection.

    PER-ROW HOLIDAY REGIME: For EACH input date the appropriate holiday list
    ("old" vs. "new") is selected by comparing to the transition date
    ``2023-12-26`` (``TRANSITION_DATE``). Dates strictly before the transition
    use the old list; dates on or after it use the new list. This mirrors the
    behavior of ``count`` and ``offset`` which apply regime logic element-wise.

    ORDER & SHAPE PRESERVATION: The output preserves the original element order.
    No sorting, deduplication, reshaping or alignment is performed; the i-th
    result corresponds to the i-th provided date after broadcasting (if any
    broadcasting occurred from a scalar input elsewhere in the call chain).

    NULL PROPAGATION: A null scalar argument short-circuits to ``None``. Null
    values inside array-like inputs produce nulls at the corresponding output
    positions.

    RETURN TYPE: If the (non-null) input resolves to a single element a Python
    ``bool`` is returned. If that lone element is null, ``None`` is returned.
    Otherwise a ``polars.Series`` of booleans named ``'is_bday'`` is produced.

    WEEKENDS: Saturdays and Sundays are never business days regardless of the
    holiday regime.

    Args:
        dates: Single date or collection (list/tuple/ndarray/Polars/Pandas
            Series). May include nulls which propagate. Null scalar input
            returns ``None``.

    Returns:
        bool | pl.Series | None: ``True`` if business day, ``False`` otherwise
        for scalar input; ``None`` for null scalar input; or a Polars boolean
        Series (name: ``'is_bday'``) for array inputs.

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
        - Transition date defined in ``TRANSITION_DATE``.
        - Mirrors per-row logic used in ``count`` and ``offset``.
        - Weekends always evaluate to ``False``.
        - Null elements propagate.
    """
    # Build DataFrame to allow conditional expression selecting the right holiday list
    df = pl.DataFrame(
        {"dates": cv.convert_dates(dates)},
        schema={"dates": pl.Date},
        nan_to_null=True,
    )

    is_bday_expr = (
        pl.when(pl.col("dates") < TRANSITION_DATE)
        .then(
            pl.col("dates").dt.is_business_day(holidays=OLD_HOLIDAYS_ARRAY),
        )
        .otherwise(
            pl.col("dates").dt.is_business_day(holidays=NEW_HOLIDAYS_ARRAY),
        )
        .alias("is_bday")
    )

    s = df.select(is_bday_expr)["is_bday"]

    if not tp.has_array_like_args(dates):
        return s.first()

    return s


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
    bz_today = clock.today()
    result = offset(bz_today, 0, roll="backward")
    assert isinstance(result, dt.date), (
        "Assumption violated: offset did not return a date for the current date."
    )
    return result
