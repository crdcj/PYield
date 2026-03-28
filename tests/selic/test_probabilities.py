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


# Mapeamento para renomear colunas do parquet antigo (inglês) para português
_RENOMEAR_COLUNAS = {
    "TradeDate": "data_referencia",
    "TickerSymbol": "codigo_negociacao",
    "MeetingEndDate": "data_fim_reuniao",
    "ExpiryDate": "data_expiracao",
    "OptionType": "tipo_opcao",
    "StrikeChangeBps": "variacao_strike_bps",
    "SettlementPrice": "preco_ajuste",
    "BDaysToExp": "dias_uteis",
}


@pytest.fixture(scope="module")
def cpm_fixture() -> pl.DataFrame:
    df = pl.read_parquet(DATA / "cpm_29012025.parquet")
    return df.rename(_RENOMEAR_COLUNAS, strict=False)


@pytest.fixture
def patched_cpm(monkeypatch, cpm_fixture):
    monkeypatch.setattr(_cpm, "data", lambda _date: cpm_fixture)
    monkeypatch.setattr(
        probs.di1,
        "interpolate_rates",
        lambda *a, **kw: pl.Series("taxa_interpolada", [0.0] * len(a[0])),
    )
    return cpm_fixture


# ── Empty schema ──────────────────────────────────────────────────────────


def test_empty_schema_zero_rows():
    df = probs._empty_schema()
    assert len(df) == 0


def test_empty_schema_columns():
    expected = [
        "data_referencia",
        "data_fim_reuniao",
        "data_expiracao",
        "ranking_reuniao",
        "variacao_strike_bps",
        "dias_uteis",
        "preco_ajuste",
        "taxa_di1",
        "fator_desconto",
        "prob_bruta",
        "prob",
        "prob_acumulada",
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
    sums = df.group_by("data_expiracao").agg(pl.col("prob").sum())
    assert (sums["prob"] - 1.0).abs().max() < 1e-9


def test_cum_prob_ends_at_one(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    last = (
        df.sort(["data_expiracao", "variacao_strike_bps"])
        .group_by("data_expiracao")
        .agg(pl.col("prob_acumulada").last())
    )
    assert (last["prob_acumulada"] - 1.0).abs().max() < 1e-9


def test_raw_prob_non_negative(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    assert (df["prob_bruta"] >= 0.0).all()


def test_prob_non_negative(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    assert (df["prob"] >= 0.0).all()


def test_cum_prob_monotone(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    for expiry in df["data_expiracao"].unique().to_list():
        sub = df.filter(pl.col("data_expiracao") == expiry).sort("variacao_strike_bps")
        diffs = sub["prob_acumulada"].diff().drop_nulls()
        assert (diffs >= -1e-12).all(), f"prob_acumulada not monotone for {expiry}"


# ── MeetingRank ───────────────────────────────────────────────────────────


def test_meeting_rank_starts_at_one(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    assert df["ranking_reuniao"].min() == 1


def test_meeting_rank_consecutive(patched_cpm):
    df = probs.all_meetings("29-01-2025")
    ranks = df["ranking_reuniao"].unique().sort().to_list()
    assert ranks == list(range(1, len(ranks) + 1))


# ── Null-price meetings excluded ─────────────────────────────────────────


def test_null_price_meeting_excluded(monkeypatch, cpm_fixture):
    """A meeting where all strikes have null preco_ajuste is excluded."""
    null_meeting = cpm_fixture.with_columns(
        pl.when(pl.col("codigo_negociacao").str.starts_with("CPMK25"))
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col("preco_ajuste"))
        .alias("preco_ajuste")
    )
    monkeypatch.setattr(_cpm, "data", lambda _: null_meeting)
    monkeypatch.setattr(
        probs.di1,
        "interpolate_rates",
        lambda *a, **kw: pl.Series("taxa_interpolada", [0.0] * len(a[0])),
    )
    df = probs.all_meetings("29-01-2025")
    assert datetime.date(2025, 5, 8) not in df["data_expiracao"].to_list()
    assert df["ranking_reuniao"].min() == 1


# ── meeting() selection ───────────────────────────────────────────────────


def test_meeting_nearest_is_rank_one(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert df["ranking_reuniao"].unique().to_list() == [1]


def test_meeting_nearest_single_expiry(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert df["data_expiracao"].n_unique() == 1


def test_meeting_explicit_expiration(patched_cpm):
    df_all = probs.all_meetings("29-01-2025")
    max_rank = df_all["ranking_reuniao"].max()
    second_expiry = df_all.filter(pl.col("ranking_reuniao") == max_rank)[
        "data_expiracao"
    ][0]
    df = probs.meeting("29-01-2025", expiration=second_expiry)
    assert df["data_expiracao"].unique().item() == second_expiry


def test_meeting_rank_always_one(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert (df["ranking_reuniao"] == 1).all()


def test_meeting_prob_sums_to_one(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert abs(df["prob"].sum() - 1.0) < 1e-9


# ── Spot checks ───────────────────────────────────────────────────────────


def test_nearest_meeting_expiry_date(patched_cpm):
    df = probs.meeting("29-01-2025")
    assert df["data_expiracao"].unique().item() == datetime.date(2025, 1, 30)


def test_highest_prob_strike_jan2025(patched_cpm):
    """On 2025-01-29, +100 bps was the overwhelmingly dominant strike."""
    df = probs.meeting("29-01-2025")
    top_strike = df.sort("prob", descending=True)["variacao_strike_bps"][0]
    assert top_strike == 100


def test_discount_exp_one_when_rate_zero(patched_cpm):
    """With di1 patched to return 0.0, fator_desconto must equal 1.0."""
    df = probs.all_meetings("29-01-2025")
    assert (df["fator_desconto"] - 1.0).abs().max() < 1e-12


def test_raw_prob_equals_settlement_over_100_when_rate_zero(patched_cpm):
    """With fator_desconto=1.0, prob_bruta = preco_ajuste / 100."""
    df = probs.all_meetings("29-01-2025")
    expected = df["preco_ajuste"] / 100
    diff = (df["prob_bruta"] - expected).abs().max()
    assert diff < 1e-12
