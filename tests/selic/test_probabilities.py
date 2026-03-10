"""
Tests for pyield.selic.probabilities — implied COPOM probabilities.

All tests monkeypatch cpm.data() and di1.interpolate_rate() via the
local patched_cpm fixture. No live network calls.
"""

import datetime
from pathlib import Path

import polars as pl
import pytest

import pyield.selic.cpm as _cpm
import pyield.selic.probabilities as probs

DATA = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def cpm_fixture() -> pl.DataFrame:
    return pl.read_parquet(DATA / "cpm_29012025.parquet")


@pytest.fixture
def patched_cpm(monkeypatch, cpm_fixture):
    monkeypatch.setattr(_cpm, "data", lambda _date: cpm_fixture)
    monkeypatch.setattr(
        probs.di1,
        "interpolate_rates",
        lambda *a, **kw: pl.Series("FlatFwdRate", [0.0] * len(a[0])),
    )
    return cpm_fixture


# ── Empty schema ──────────────────────────────────────────────────────────


def test_empty_schema_zero_rows():
    df = probs._empty_schema()
    assert len(df) == 0


def test_empty_schema_columns():
    expected = [
        "TradeDate",
        "MeetingEndDate",
        "ExpiryDate",
        "MeetingRank",
        "StrikeChangeBps",
        "BDaysToExp",
        "SettlementPrice",
        "DI1Rate",
        "DiscountExp",
        "RawProb",
        "Prob",
        "CumProb",
    ]
    assert probs._empty_schema().columns == expected


# ── Empty input propagation ───────────────────────────────────────────────


def test_all_meetings_empty_input(monkeypatch):
    monkeypatch.setattr(_cpm, "data", lambda _: _cpm._empty_schema())
    result = probs.all_meetings("01-01-2025")
    assert result.is_empty()
    assert result.columns == probs._empty_schema().columns


def test_meeting_empty_input(monkeypatch):
    monkeypatch.setattr(_cpm, "data", lambda _: _cpm._empty_schema())
    result = probs.meeting("01-01-2025")
    assert result.is_empty()
    assert result.columns == probs._empty_schema().columns


# ── Schema of non-empty output ────────────────────────────────────────────


def test_all_meetings_schema(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    assert df.columns == probs._empty_schema().columns


# ── Probability invariants ────────────────────────────────────────────────


def test_prob_sums_to_one(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    sums = df.group_by("ExpiryDate").agg(pl.col("Prob").sum())
    assert (sums["Prob"] - 1.0).abs().max() < 1e-9


def test_cum_prob_ends_at_one(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    last = (
        df.sort(["ExpiryDate", "StrikeChangeBps"])
        .group_by("ExpiryDate")
        .agg(pl.col("CumProb").last())
    )
    assert (last["CumProb"] - 1.0).abs().max() < 1e-9


def test_raw_prob_non_negative(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    assert (df["RawProb"] >= 0.0).all()


def test_prob_non_negative(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    assert (df["Prob"] >= 0.0).all()


def test_cum_prob_monotone(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    for expiry in df["ExpiryDate"].unique().to_list():
        sub = df.filter(pl.col("ExpiryDate") == expiry).sort("StrikeChangeBps")
        diffs = sub["CumProb"].diff().drop_nulls()
        assert (diffs >= -1e-12).all(), f"CumProb not monotone for {expiry}"


# ── MeetingRank ───────────────────────────────────────────────────────────


def test_meeting_rank_starts_at_one(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    assert df["MeetingRank"].min() == 1


def test_meeting_rank_consecutive(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    ranks = df["MeetingRank"].unique().sort().to_list()
    assert ranks == list(range(1, len(ranks) + 1))


# ── Null-price meetings excluded ─────────────────────────────────────────


def test_null_price_meeting_excluded(monkeypatch, cpm_fixture):
    """A meeting where all strikes have null SettlementPrice is excluded."""
    null_meeting = cpm_fixture.with_columns(
        pl.when(pl.col("TickerSymbol").str.starts_with("CPMK25"))
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col("SettlementPrice"))
        .alias("SettlementPrice")
    )
    monkeypatch.setattr(_cpm, "data", lambda _: null_meeting)
    monkeypatch.setattr(
        probs.di1,
        "interpolate_rates",
        lambda *a, **kw: pl.Series("FlatFwdRate", [0.0] * len(a[0])),
    )
    df = probs.all_meetings("29-01-2025")
    assert datetime.date(2025, 5, 8) not in df["ExpiryDate"].to_list()
    assert df["MeetingRank"].min() == 1


# ── meeting() selection ───────────────────────────────────────────────────


def test_meeting_nearest_is_rank_one(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert df["MeetingRank"].unique().to_list() == [1]


def test_meeting_nearest_single_expiry(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert df["ExpiryDate"].n_unique() == 1


def test_meeting_explicit_expiration(patched_cpm):
    df_all = probs.all_meetings("29-01-2025")
    max_rank = df_all["MeetingRank"].max()
    second_expiry = df_all.filter(pl.col("MeetingRank") == max_rank)["ExpiryDate"][0]
    df = probs.meeting("29-01-2025", expiration=second_expiry)
    assert df["ExpiryDate"].unique().item() == second_expiry


def test_meeting_rank_always_one(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert (df["MeetingRank"] == 1).all()


def test_meeting_prob_sums_to_one(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert abs(df["Prob"].sum() - 1.0) < 1e-9


# ── Spot checks ───────────────────────────────────────────────────────────


def test_nearest_meeting_expiry_date(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert df["ExpiryDate"].unique().item() == datetime.date(2025, 1, 30)


def test_highest_prob_strike_jan2025(patched_cpm):
    """On 2025-01-29, +100 bps was the overwhelmingly dominant strike."""
    df = probs.meeting("29-01-2025")
    top_strike = df.sort("Prob", descending=True)["StrikeChangeBps"][0]
    assert top_strike == 100


def test_discount_exp_one_when_rate_zero(patched_cpm):
    """With di1 patched to return 0.0, DiscountExp must equal 1.0."""
    df = probs.all_meetings("29-01-2025")
    assert (df["DiscountExp"] - 1.0).abs().max() < 1e-12


def test_raw_prob_equals_settlement_over_100_when_rate_zero(patched_cpm):
    """With DiscountExp=1.0, RawProb = SettlementPrice / 100."""
    df = probs.all_meetings("29-01-2025")
    expected = df["SettlementPrice"] / 100
    diff = (df["RawProb"] - expected).abs().max()
    assert diff < 1e-12
