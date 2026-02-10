"""Tests for the Polars expression builders (pyield.bday.expr)."""

import datetime as dt

import polars as pl

from pyield import bday
from pyield.bday import count_expr, is_business_day_expr, offset_expr

# ---------------------------------------------------------------------------
# is_business_day_expr
# ---------------------------------------------------------------------------


def test_is_business_day_basic():
    """Weekday that is not a holiday should return True."""
    df = pl.DataFrame({"d": [dt.date(2024, 1, 2)]})  # Tuesday
    result = df.select(is_business_day_expr(pl.col("d")))
    assert result["d"].to_list() == [True]


def test_is_business_day_weekend():
    """Saturday and Sunday should return False."""
    df = pl.DataFrame({"d": [dt.date(2024, 1, 6), dt.date(2024, 1, 7)]})
    result = df.select(is_business_day_expr(pl.col("d")))
    assert result["d"].to_list() == [False, False]


def test_is_business_day_holiday():
    """Christmas 2024 (new regime) is not a business day."""
    df = pl.DataFrame({"d": [dt.date(2024, 12, 25)]})
    result = df.select(is_business_day_expr(pl.col("d")))
    assert result["d"].to_list() == [False]


def test_is_business_day_old_vs_new_regime():
    """Nov 20 (Zumbi dos Palmares) is a holiday only in the new regime.

    2020-11-20 (old regime): business day
    2024-11-20 (new regime): NOT a business day
    """
    df = pl.DataFrame(
        {
            "d": [dt.date(2020, 11, 20), dt.date(2024, 11, 20)],
        }
    )
    result = df.select(is_business_day_expr(pl.col("d")))
    assert result["d"].to_list() == [True, False]


def test_is_business_day_null_propagation():
    """Null dates should propagate as null."""
    df = pl.DataFrame({"d": [dt.date(2024, 1, 2), None]}, schema={"d": pl.Date})
    result = df.select(is_business_day_expr(pl.col("d")))
    assert result["d"].to_list() == [True, None]


def test_is_business_day_string_and_invalid_inputs():
    """String dates should parse; invalid values should become null."""
    df = pl.DataFrame({"d": ["02-01-2024", "03/01/2024", "2024-01-06", "31-02-2024"]})
    result = df.select(is_business_day_expr(pl.col("d")))
    assert result["d"].to_list() == [True, True, False, None]


def test_is_business_day_matches_core():
    """Results should match pyield.bday.is_business_day for the same inputs."""

    dates = [
        dt.date(2020, 11, 20),
        dt.date(2024, 11, 20),
        dt.date(2024, 1, 1),
        dt.date(2024, 7, 15),
    ]
    df = pl.DataFrame({"d": dates})
    expr_result = df.select(is_business_day_expr(pl.col("d")))["d"].to_list()
    core_result = bday.is_business_day(dates).to_list()
    assert expr_result == core_result


# ---------------------------------------------------------------------------
# offset_expr
# ---------------------------------------------------------------------------


def test_offset_positive():
    """Offset by +5 business days."""
    df = pl.DataFrame({"d": [dt.date(2024, 1, 2)]})  # Tuesday
    result = df.select(offset_expr(pl.col("d"), 5))
    assert result["d"].to_list() == [dt.date(2024, 1, 9)]


def test_offset_negative():
    """Offset by -1 business day."""
    df = pl.DataFrame({"d": [dt.date(2024, 1, 3)]})  # Wednesday
    result = df.select(offset_expr(pl.col("d"), -1))
    assert result["d"].to_list() == [dt.date(2024, 1, 2)]


def test_offset_zero_on_business_day():
    """Offset 0 on a business day returns the same date."""
    df = pl.DataFrame({"d": [dt.date(2024, 1, 2)]})
    result = df.select(offset_expr(pl.col("d"), 0))
    assert result["d"].to_list() == [dt.date(2024, 1, 2)]


def test_offset_zero_on_weekend_forward():
    """Offset 0 on Saturday with forward roll goes to Monday."""
    df = pl.DataFrame({"d": [dt.date(2024, 1, 6)]})  # Saturday
    result = df.select(offset_expr(pl.col("d"), 0, roll="forward"))
    assert result["d"].to_list() == [dt.date(2024, 1, 8)]  # Monday


def test_offset_zero_on_weekend_backward():
    """Offset 0 on Saturday with backward roll goes to Friday."""
    df = pl.DataFrame({"d": [dt.date(2024, 1, 6)]})  # Saturday
    result = df.select(offset_expr(pl.col("d"), 0, roll="backward"))
    assert result["d"].to_list() == [dt.date(2024, 1, 5)]  # Friday


def test_offset_old_vs_new_regime():
    """Nov 20 is a holiday only in new regime; offset(0) should differ."""
    df = pl.DataFrame(
        {
            "d": [dt.date(2020, 11, 20), dt.date(2024, 11, 20)],
        }
    )
    result = df.select(offset_expr(pl.col("d"), 0))
    # 2020-11-20: Friday, not a holiday (old regime) -> stays
    # 2024-11-20: Wednesday, holiday (new regime) -> rolls to 2024-11-21
    assert result["d"].to_list() == [dt.date(2020, 11, 20), dt.date(2024, 11, 21)]


def test_offset_with_expr_n():
    """Offset with a column-based n (pl.Expr)."""
    df = pl.DataFrame(
        {
            "d": [dt.date(2024, 1, 2), dt.date(2024, 1, 2)],
            "n": [1, 5],
        }
    )
    result = df.select(offset_expr(pl.col("d"), pl.col("n")))
    assert result["d"].to_list() == [dt.date(2024, 1, 3), dt.date(2024, 1, 9)]


def test_offset_null_propagation():
    """Null dates should propagate as null."""
    df = pl.DataFrame({"d": [dt.date(2024, 1, 2), None]}, schema={"d": pl.Date})
    result = df.select(offset_expr(pl.col("d"), 1))
    assert result["d"].to_list() == [dt.date(2024, 1, 3), None]


def test_offset_invalid_string_date_becomes_null():
    """Invalid string dates should not raise and must become null."""
    df = pl.DataFrame({"d": ["02-01-2024", "31-02-2024", ""]})
    result = df.select(offset_expr(pl.col("d"), 0))
    assert result["d"].to_list() == [dt.date(2024, 1, 2), None, None]


def test_offset_matches_core():
    """Results should match pyield.bday.offset for the same inputs."""

    dates = [
        dt.date(2020, 11, 20),
        dt.date(2024, 11, 20),
        dt.date(2024, 1, 1),
        dt.date(2024, 7, 15),
    ]
    df = pl.DataFrame({"d": dates})
    expr_result = df.select(offset_expr(pl.col("d"), 5))["d"].to_list()
    core_result = bday.offset(dates, 5).to_list()
    assert expr_result == core_result


# ---------------------------------------------------------------------------
# count_expr
# ---------------------------------------------------------------------------


def test_count_basic():
    """Count business days in a simple period."""
    df = pl.DataFrame(
        {
            "start": [dt.date(2024, 1, 2)],
            "end": [dt.date(2024, 1, 9)],
        }
    )
    result = df.select(count_expr(pl.col("start"), pl.col("end")))
    assert result["start"].to_list() == [5]


def test_count_with_scalar_end():
    """Count with a scalar dt.date as end."""
    df = pl.DataFrame({"start": [dt.date(2024, 1, 2), dt.date(2024, 1, 3)]})
    result = df.select(count_expr(pl.col("start"), dt.date(2024, 1, 9)))
    assert result["start"].to_list() == [5, 4]


def test_count_negative():
    """Count is negative when end < start."""
    df = pl.DataFrame(
        {
            "start": [dt.date(2024, 1, 9)],
            "end": [dt.date(2024, 1, 2)],
        }
    )
    result = df.select(count_expr(pl.col("start"), pl.col("end")))
    assert result["start"].to_list() == [-5]


def test_count_old_vs_new_regime():
    """Nov 20 (Zumbi dos Palmares) counts differ between regimes."""
    df = pl.DataFrame(
        {
            "start": [dt.date(2020, 11, 20), dt.date(2024, 11, 20)],
            "end": [dt.date(2020, 11, 21), dt.date(2024, 11, 21)],
        }
    )
    result = df.select(count_expr(pl.col("start"), pl.col("end")))
    # Old regime: Nov 20 is a weekday, not a holiday -> 1 bday
    # New regime: Nov 20 is a holiday -> 0 bdays
    assert result["start"].to_list() == [1, 0]


def test_count_null_propagation():
    """Null dates should propagate as null."""
    df = pl.DataFrame(
        {"start": [dt.date(2024, 1, 2), None], "end": [dt.date(2024, 1, 9), None]},
        schema={"start": pl.Date, "end": pl.Date},
    )
    result = df.select(count_expr(pl.col("start"), pl.col("end")))
    assert result["start"].to_list() == [5, None]


def test_count_with_string_columns_and_invalid_values():
    """String date columns should parse with invalid values mapped to null."""
    df = pl.DataFrame(
        {
            "start": ["02-01-2024", "03/01/2024", "31-02-2024"],
            "end": ["2024-01-09", "2024-01-10", "2024-01-10"],
        }
    )
    result = df.select(count_expr(pl.col("start"), pl.col("end")))
    assert result["start"].to_list() == [5, 5, None]


def test_count_dtype_is_int64():
    """Result dtype should be Int64."""
    df = pl.DataFrame(
        {
            "start": [dt.date(2024, 1, 2)],
            "end": [dt.date(2024, 1, 9)],
        }
    )
    result = df.select(count_expr(pl.col("start"), pl.col("end")))
    assert result["start"].dtype == pl.Int64


def test_count_matches_core():
    """Results should match pyield.bday.count for the same inputs."""

    starts = [
        dt.date(2020, 11, 20),
        dt.date(2024, 11, 20),
        dt.date(2024, 1, 1),
        dt.date(2024, 7, 15),
    ]
    ends = [
        dt.date(2020, 11, 21),
        dt.date(2024, 11, 21),
        dt.date(2024, 1, 31),
        dt.date(2024, 8, 15),
    ]
    df = pl.DataFrame({"start": starts, "end": ends})
    expr_result = df.select(count_expr(pl.col("start"), pl.col("end")))[
        "start"
    ].to_list()
    core_result = bday.count(starts, ends).to_list()
    assert expr_result == core_result


# ---------------------------------------------------------------------------
# Integration: with_columns / chaining
# ---------------------------------------------------------------------------


def test_with_columns_all_methods():
    """All three expression builders used together in a single with_columns call."""
    df = pl.DataFrame(
        {
            "date": [dt.date(2024, 1, 2), dt.date(2024, 1, 3)],
            "end": [dt.date(2024, 1, 9), dt.date(2024, 1, 9)],
        }
    )
    result = df.with_columns(
        is_business_day_expr(pl.col("date")).alias("is_bd"),
        offset_expr(pl.col("date"), 5).alias("next_bday"),
        count_expr(pl.col("date"), pl.col("end")).alias("bdays"),
    )
    assert result["is_bd"].to_list() == [True, True]
    assert result["next_bday"].to_list() == [dt.date(2024, 1, 9), dt.date(2024, 1, 10)]
    assert result["bdays"].to_list() == [5, 4]


def test_lazy_frame():
    """Expression builders work with LazyFrame -> collect."""
    lf = pl.LazyFrame(
        {
            "d": [dt.date(2024, 1, 2), dt.date(2024, 1, 6)],
        }
    )
    result = lf.select(is_business_day_expr(pl.col("d"))).collect()
    assert result["d"].to_list() == [True, False]
