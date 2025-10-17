import datetime as dt
from typing import Literal, overload

import numpy as np
import pandas as pd
import polars as pl

import pyield.bday.holidays as hl
import pyield.converters as cv
from pyield.config import TIMEZONE_BZ
from pyield.types import DateArray, DateScalar

IntegerScalar = int | np.integer
IntegerArray = np.ndarray | list | tuple | pl.Series | pd.Series


# Initialize Brazilian holidays data
br_holidays = hl.BrHolidays()
OLD_HOLIDAYS_ARRAY = br_holidays.get_holiday_array(holiday_option="old")
NEW_HOLIDAYS_ARRAY = br_holidays.get_holiday_array(holiday_option="new")
TRANSITION_DATE = br_holidays.TRANSITION_DATE


@overload
def offset_np(
    dates: DateScalar, offset: IntegerScalar, roll: Literal["forward", "backward"] = ...
) -> dt.date | None: ...
@overload
def offset_np(
    dates: DateArray,
    offset: IntegerArray | IntegerScalar,
    roll: Literal["forward", "backward"] = ...,
) -> pd.Series: ...
@overload
def offset_np(
    dates: DateScalar, offset: IntegerArray, roll: Literal["forward", "backward"] = ...
) -> pd.Series: ...


def offset_np(  # noqa
    dates: DateScalar | DateArray,
    offset: IntegerScalar | IntegerArray,
    roll: Literal["forward", "backward"] = "forward",
) -> dt.date | pd.Series | None:
    """
    First adjusts the date to fall on a valid day according to the roll rule, then
    applies offsets to the given dates to the next or previous business day, considering
    brazilian holidays. This function supports both single dates and collections of
    dates. It is a wrapper for `numpy.busday_offset` adapted for Pandas data types and
    brazilian holidays.

    **Important Note:** Each date in the `dates` input is evaluated individually to
    determine which list of holidays applies to the calculation. Transition date
    is 2023-12-26, which means:
    - Dates before 2023-12-26 use the old holiday list.
    - Dates on or after 2023-12-26 use the new holiday list.

    Args:
        dates (DateScalar | DateArray): The date(s) to offset. Can be a scalar date type
            or a collection of dates. **Transition Handling:** Due to a change in
            Brazilian national holidays effective from 2023-12-26 (`TRANSITION_DATE`),
            this function automatically selects the appropriate holiday list
            (old or new) based on **each individual date** in the `dates` input.
            Dates before 2023-12-26 use the old list for their offset calculation,
            while dates on or after 2023-12-26 use the new list.
        offset (int | Series | np.ndarray | list[int] | tuple[int], optional):
            The number of business days to offset the dates. Positive for future dates,
            negative for past dates. Zero will return the same date if it's a business
            day, or the next business day otherwise.
        roll (Literal["forward", "backward"], optional): Direction to roll the date if
            it falls on a holiday or weekend. 'forward' to the next business day,
            'backward' to the previous. Defaults to 'forward'.

    Returns:
        dt.date | pd.Series: If a single date is provided, returns a single
            `date` of the offset date. If a series of dates is provided, returns a
            `Series` of offset dates.

    Examples:
        >>> from pyield import bday

        Offset to the next business day if not a bday (offset=0 and roll="forward")

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

        List of dates and offsets
        >>> bday.offset(["19-09-2024", "20-09-2024"], 1)  # a list of dates
        0   2024-09-20
        1   2024-09-23
        dtype: date32[day][pyarrow]

        >>> bday.offset("19-09-2024", [1, 2])  # a list of offsets
        0   2024-09-20
        1   2024-09-23
        dtype: date32[day][pyarrow]

        Scalar null values are propagated
        >>> bday.offset(pd.NaT, 1)  # NaT input

        >>> bday.offset(pd.NaT, [1, 2])  # NaT input with a list of offsets
        0   <NA>
        1   <NA>
        dtype: date32[day][pyarrow]

        >>> bday.offset(["19-09-2024", pd.NaT], 1)  # NaT in a list of dates
        0   2024-09-20
        1   <NA>
        dtype: date32[day][pyarrow]

        Original pandas index is preserved
        >>> dates = pd.Series(
        ...     ["19-09-2024", "20-09-2024", "21-09-2024"],
        ...     index=["a", "b", "c"],
        ... )
        >>> bday.offset(dates, 1)
        a   2024-09-20
        b   2024-09-23
        c   2024-09-24
        dtype: date32[day][pyarrow]

    Note:
        This function uses `numpy.busday_offset` under the hood, which means it follows
        the same conventions and limitations for business day calculations. For detailed
        information on error handling and behavior, refer to the `numpy.busday_offset`
        documentation:
        https://numpy.org/doc/stable/reference/generated/numpy.busday_offset.html
    """
    # Padroniza as entradas para objetos do Pandas
    dates_pd = cv.convert_dates(dates)
    if isinstance(offset, IntegerArray):
        offset_pd = pd.Series(offset).astype("int64[pyarrow]")
    else:
        offset_pd = offset

    # 1. VALIDAÇÃO ESTRUTURAL
    if isinstance(dates_pd, pd.Series) and isinstance(offset_pd, pd.Series):
        dates_pd = dates_pd.to_pandas()
        if not dates_pd.index.equals(offset_pd.index):
            raise ValueError("Input dates have different indices!")

    # 2. LÓGICA PRINCIPAL

    # --- CASO 1: AMBOS OS INPUTS SÃO ESCALARES ---
    if not isinstance(dates_pd, pd.Series) and not isinstance(offset_pd, pd.Series):
        if pd.isna(dates_pd) or pd.isna(offset_pd):
            return None

        offsetted_date_np = np.busday_offset(
            dates=dates_pd,
            offsets=offset_pd,
            roll=roll,
            holidays=br_holidays.get_holiday_array(dates_pd),
        )

        assert isinstance(offsetted_date_np, np.datetime64), (
            "Assumption violated: np.busday_offset did not return a scalar "
            f"np.datetime64 for scalar inputs. Got: {type(offsetted_date_np)}"
        )
        return offsetted_date_np.item()

    # --- CASO 2: PELO MENOS UM DOS INPUTS É UMA SÉRIE ---
    # Garante que ambas as variáveis sejam Series para alinhamento automático.
    # O Pandas faz o "broadcast" do escalar para o tamanho da Series.
    if isinstance(offset_pd, pd.Series):
        dates_pd = pd.Series(dates_pd, index=offset_pd.index, dtype="date32[pyarrow]")
    else:
        offset_pd = pd.Series(offset_pd, index=dates_pd.index)

    # Inicializa a série de resultados com o índice correto e tipo que suporta NaT.
    result = pd.Series(index=dates_pd.index, dtype="date32[pyarrow]")

    # Divide the input in order to apply the correct holiday list
    mask_not_null = dates_pd.notna() & offset_pd.notna()
    mask_old_holidays = mask_not_null & (dates_pd < TRANSITION_DATE)
    mask_new_holidays = mask_not_null & (dates_pd >= TRANSITION_DATE)

    if mask_old_holidays.any():
        np_offset_values = np.busday_offset(
            dates=cv.to_numpy_date_type(dates_pd[mask_old_holidays]),
            offsets=offset_pd[mask_old_holidays],
            roll=roll,
            holidays=OLD_HOLIDAYS_ARRAY,
        )
        pd_offset_values = pd.to_datetime(np_offset_values).astype("date32[pyarrow]")
        result.loc[mask_old_holidays] = pd_offset_values

    if mask_new_holidays.any():
        np_offset_values = np.busday_offset(
            dates=cv.to_numpy_date_type(dates_pd[mask_new_holidays]),
            offsets=offset_pd[mask_new_holidays],
            roll=roll,
            holidays=NEW_HOLIDAYS_ARRAY,
        )
        pd_offset_values = pd.to_datetime(np_offset_values).astype("date32[pyarrow]")
        result.loc[mask_new_holidays] = pd_offset_values

    return result


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
    Counts the number of business days between a `start` date (inclusive) and an `end`
    date (exclusive). The function can handle single dates, arrays of dates and
    mixed inputs, returning either a single integer or a series of integers depending
    on the inputs. It accounts for specified holidays, effectively excluding them from
    the business day count.

    **Important Note:** Each date in the `start` input is evaluated individually to
    determine which list of holidays (old or new) applies to the calculation. The
    transition date is 2023-12-26, which means:
    - Dates before 2023-12-26 use the old holiday list.
    - Dates on or after 2023-12-26 use the new holiday list.

    Args:
        start (DateScalar | DateArray): The start date(s) for counting (inclusive).
            **Transition Handling:** The holiday list used for the *entire* counting
            period between `start` and `end` is determined solely by the `start` date's
            relation to the holiday transition date (2023-12-26). If `start` is before
            this date, the old holiday list is used for the whole count, even if `end`
            is after it. If `start` is on or after this date, new holiday list is used.
        end (DateScalar | DateArray): The end date(s) for counting (exclusive).

    Returns:
        int | pd.Series | None: Returns an integer or None if `start` and `end` are
            single dates, or a Series if any of them is an array of dates.

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

        Total business days in January and February since the start of the year
        >>> bday.count(start="01-01-2024", end=["01-02-2024", "01-03-2024"])
        shape: (2,)
        Series: 'result' [i32]
        [
            22
            41
        ]

        The remaining business days from January/February until the end of the year
        >>> bday.count(["01-01-2024", "01-02-2024"], "01-01-2025")
        shape: (2,)
        Series: 'result' [i32]
        [
            253
            231
        ]

        The total business days in January and February of 2024
        >>> bday.count(["01-01-2024", "01-02-2024"], ["01-02-2024", "01-03-2024"])
        shape: (2,)
        Series: 'result' [i32]
        [
            22
            19
        ]

        Null values are propagated
        >>> bday.count(None, "01-01-2024")  # None start

        >>> bday.count("01-01-2024", None)  # None end

        >>> bday.count("01-01-2024", ["01-02-2024", None])  # None in end array
        shape: (2,)
        Series: 'result' [i32]
        [
            22
            Null
        ]

        >>> start_dates = pd.Series(["01-01-2024", "01-02-2024", "01-03-2024"])
        >>> bday.count(start_dates, "01-01-2025")
        shape: (3,)
        Series: 'result' [i32]
        [
            253
            231
            212
        ]
    """
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
    )

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
            Specifies the list of holidays to consider. Defaults to "new".
            - **'old'**: Uses the holiday list effective *before* the transition date
            of 2023-12-26.
            - **'new'**: Uses the holiday list effective *on and after* the transition
            date of 2023-12-26.
            - **'infer'**: Automatically selects the holiday list ('old' or 'new') based
            on the `start` date relative to the transition date (2023-12-26). If `start`
            is before the transition, 'old' is used; otherwise, 'new' is used.

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
        dtype: date32[day][pyarrow]

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

    return pd.Series(result_dti.values).astype("date32[pyarrow]")


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

    Notes:
        - This function correctly identifies business days according to the applicable
        Brazilian holiday list (before or after the 2023-12-26 transition), based
        on the input `date`.
    """
    date_pd = cv.convert_dates(date)
    shifted_date = offset(date_pd, 0)  # Shift the date if it is not a business day
    return date_pd == shifted_date


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
    First adjusts the date to fall on a valid day according to the roll rule, then
    applies offsets to the given dates to the next or previous business day, considering
    brazilian holidays. This function supports both single dates and collections of
    dates. It is a wrapper for `polars.Expr.dt.add_business_days`.

    **Important Note:** Each date in the `dates` input is evaluated individually to
    determine which list of holidays applies to the calculation. Transition date
    is 2023-12-26, which means:
    - Dates before 2023-12-26 use the old holiday list.
    - Dates on or after 2023-12-26 use the new holiday list.

    Args:
        dates (DateScalar | DateArray): The date(s) to offset. Can be a scalar date type
            or a collection of dates. **Transition Handling:** Due to a change in
            Brazilian national holidays effective from 2023-12-26 (`TRANSITION_DATE`),
            this function automatically selects the appropriate holiday list
            (old or new) based on **each individual date** in the `dates` input.
            Dates before 2023-12-26 use the old list for their offset calculation,
            while dates on or after 2023-12-26 use the new list.
        offset (int | Series | np.ndarray | list[int] | tuple[int], optional):
            The number of business days to offset the dates. Positive for future dates,
            negative for past dates. Zero will return the same date if it's a business
            day, or the next/previous business day otherwise, according to `roll`.
        roll (Literal["forward", "backward"], optional): Direction to roll the date if
            it falls on a holiday or weekend. 'forward' to the next business day,
            'backward' to the previous. Defaults to 'forward'.

    Returns:
        dt.date | pl.Series | None: If a single date is provided, returns a single
            `date` of the offset date or None. If a series of dates is provided, returns
            a `polars.Series` of offset dates.

    Examples:
        >>> from pyield import bday

        # Offset Saturday before Christmas to the next b. day (Tuesday after Christmas)
        >>> bday.offset("23-12-2023", 0)
        datetime.date(2023, 12, 26)

        # Offset Friday to the next business day (Friday is jumped -> Monday)
        >>> bday.offset("27-09-2024", 1)
        datetime.date(2024, 9, 30)

        # Offset to the previous business day
        >>> bday.offset("23-12-2023", 0, roll="backward")
        datetime.date(2023, 12, 22)

        # List of dates and offsets
        >>> bday.offset(["19-09-2024", "20-09-2024"], 1)
        shape: (2,)
        Series: 'result' [date]
        [
            2024-09-20
            2024-09-23
        ]

        # Null values are propagated
        >>> bday.offset(None, 1)

        >>> bday.offset(["19-09-2024", None], 1)
        shape: (2,)
        Series: 'result' [date]
        [
            2024-09-20
            null
        ]

    Note:
        This function uses `polars.Expr.dt.add_business_days` under the hood. For
        detailed information, refer to the Polars documentation.
    """
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
