"""
Tests for pyield.selic.cpm — CPM contract data and ticker parsing.

Ticker parsing tests are pure unit tests (no I/O).
Data correctness tests use the fixture parquet.
"""

import datetime
from pathlib import Path

import polars as pl
import pytest

from pyield import bday
from pyield.selic.cpm import _empty_schema, _parse_ticker

DATA = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def cpm_fixture() -> pl.DataFrame:
    return pl.read_parquet(DATA / "cpm_29012025.parquet")


# ── Ticker parsing: valid inputs ──────────────────────────────────────────


@pytest.mark.parametrize(
    "ticker,exp_month,exp_year,exp_type,exp_bps",
    [
        ("CPMF25C099500", 1, 2025, "call", -50),
        ("CPMZ25C100000", 12, 2025, "call", 0),
        ("CPMH26C099250", 3, 2026, "call", -75),
        ("CPMH26C099750", 3, 2026, "call", -25),
        ("CPMH26C100000", 3, 2026, "call", 0),
        ("CPMH26C100250", 3, 2026, "call", 25),
        ("CPMF25P099500", 1, 2025, "put", -50),
    ],
)
def test_parse_ticker_valid(ticker, exp_month, exp_year, exp_type, exp_bps):
    month, year, opt_type, strike, bps = _parse_ticker(ticker)
    assert month == exp_month
    assert year == exp_year
    assert opt_type == exp_type
    assert bps == exp_bps


# ── Ticker parsing: invalid inputs ───────────────────────────────────────


def test_parse_ticker_wrong_prefix():
    with pytest.raises(ValueError, match="Invalid CPM ticker"):
        _parse_ticker("DI1F25C099500")


def test_parse_ticker_unknown_month_code():
    with pytest.raises(ValueError, match="Unknown month code"):
        _parse_ticker("CPMA25C099500")  # A is not a valid month code


# ── Empty schema ──────────────────────────────────────────────────────────


def test_empty_schema_zero_rows():
    df = _empty_schema()
    assert len(df) == 0


def test_empty_schema_columns():
    df = _empty_schema()
    assert df.columns == [
        "TradeDate",
        "TickerSymbol",
        "MeetingEndDate",
        "ExpiryDate",
        "OptionType",
        "StrikeChangeBps",
        "SettlementPrice",
        "BDaysToExp",
    ]


def test_empty_schema_dtypes():
    df = _empty_schema()
    assert df["TradeDate"].dtype == pl.Date
    assert df["SettlementPrice"].dtype == pl.Float64
    assert df["StrikeChangeBps"].dtype == pl.Int32
    assert df["BDaysToExp"].dtype == pl.Int32


# ── Data correctness (fixture) ────────────────────────────────────────────


def test_settlement_price_range(cpm_fixture):
    non_null = cpm_fixture["SettlementPrice"].drop_nulls()
    assert (non_null >= 0.0).all()
    assert (non_null <= 100.0).all()


def test_option_type_values(cpm_fixture):
    assert cpm_fixture["OptionType"].is_in(["call", "put"]).all()


def test_strike_multiples_of_25(cpm_fixture):
    assert (cpm_fixture["StrikeChangeBps"] % 25 == 0).all()


def test_meeting_end_before_expiry(cpm_fixture):
    non_null = cpm_fixture.filter(pl.col("MeetingEndDate").is_not_null())
    assert (non_null["MeetingEndDate"] < non_null["ExpiryDate"]).all()


def test_meeting_end_date_not_null(cpm_fixture):
    assert cpm_fixture["MeetingEndDate"].null_count() == 0


def test_expiry_is_one_bday_after_meeting_end(cpm_fixture):
    for row in cpm_fixture.iter_rows(named=True):
        expected = bday.offset(row["MeetingEndDate"], 1)
        assert row["ExpiryDate"] == expected


def test_bdays_to_exp_positive(cpm_fixture):
    assert (cpm_fixture["BDaysToExp"] > 0).all()


# ── Spot checks (fixture: 2025-01-29) ────────────────────────────────────


def test_spot_cpmf25_expiry(cpm_fixture):
    row = cpm_fixture.filter(pl.col("TickerSymbol").str.starts_with("CPMF25"))
    assert row["ExpiryDate"].unique().item() == datetime.date(2025, 1, 30)


def test_spot_cpmf25_meeting_end(cpm_fixture):
    row = cpm_fixture.filter(pl.col("TickerSymbol").str.starts_with("CPMF25"))
    assert row["MeetingEndDate"].unique().item() == datetime.date(2025, 1, 29)


def test_spot_hold_strike_is_zero(cpm_fixture):
    hold = cpm_fixture.filter(pl.col("TickerSymbol") == "CPMF25C100000")
    assert len(hold) == 1
    assert hold["StrikeChangeBps"].item() == 0


def test_spot_most_negative_strike(cpm_fixture):
    min_bps = cpm_fixture.filter(
        pl.col("TickerSymbol").str.starts_with("CPMF25")
    )["StrikeChangeBps"].min()
    assert min_bps == -100


def test_spot_bdays_to_exp_cpmf25(cpm_fixture):
    bdays = cpm_fixture.filter(
        pl.col("TickerSymbol").str.starts_with("CPMF25")
    )["BDaysToExp"].unique().item()
    assert bdays == 1


def test_spot_bdays_to_exp_cpmk25(cpm_fixture):
    bdays = cpm_fixture.filter(
        pl.col("TickerSymbol").str.starts_with("CPMK25")
    )["BDaysToExp"].unique().item()
    assert bdays == 66
