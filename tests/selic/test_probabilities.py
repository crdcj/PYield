"""Testes de pyield.selic.probabilities.

Todos os testes fazem monkeypatch de cpm.data() e di1.interpolar_taxas()
via a fixture local cpm_patchado. Sem chamadas reais de rede.
"""

import datetime
from pathlib import Path

import polars as pl
import pytest

import pyield.selic.cpm as modulo_cpm
import pyield.selic.probabilities as modulo_probabilidades

DIRETORIO_DADOS = Path(__file__).parent / "data"


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
    df = pl.read_parquet(DIRETORIO_DADOS / "cpm_29012025.parquet")
    return df.rename(_RENOMEAR_COLUNAS, strict=False)


@pytest.fixture
def cpm_patchado(monkeypatch, cpm_fixture):
    monkeypatch.setattr(modulo_cpm, "data", lambda _date: cpm_fixture)
    monkeypatch.setattr(
        modulo_probabilidades.di1,
        "interpolar_taxas",
        lambda *a, **kw: pl.Series("taxa_interpolada", [0.0] * len(a[0])),
    )
    return cpm_fixture


# ── Empty schema ──────────────────────────────────────────────────────────


def test_empty_schema_zero_rows():
    df = modulo_probabilidades._empty_schema()
    assert len(df) == 0


def test_empty_schema_columns():
    esperado = [
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
    assert modulo_probabilidades._empty_schema().columns == esperado


# ── Empty input propagation ───────────────────────────────────────────────


def test_all_meetings_empty_input(monkeypatch):
    monkeypatch.setattr(modulo_cpm, "data", lambda _: modulo_cpm._empty_schema())
    resultado = modulo_probabilidades.all_meetings("01-01-2025")
    assert resultado.is_empty()
    assert resultado.columns == modulo_probabilidades._empty_schema().columns


def test_meeting_empty_input(monkeypatch):
    monkeypatch.setattr(modulo_cpm, "data", lambda _: modulo_cpm._empty_schema())
    resultado = modulo_probabilidades.meeting("01-01-2025")
    assert resultado.is_empty()
    assert resultado.columns == modulo_probabilidades._empty_schema().columns


# ── Schema of non-empty output ────────────────────────────────────────────


def test_all_meetings_schema(cpm_patchado):
    df = modulo_probabilidades.all_meetings("29-01-2025")
    assert df.columns == modulo_probabilidades._empty_schema().columns


# ── Probability invariants ────────────────────────────────────────────────


def test_prob_sums_to_one(cpm_patchado):
    df = modulo_probabilidades.all_meetings("29-01-2025")
    somas = df.group_by("data_expiracao").agg(pl.col("prob").sum())
    assert (somas["prob"] - 1.0).abs().max() < 1e-9


def test_cum_prob_ends_at_one(cpm_patchado):
    df = modulo_probabilidades.all_meetings("29-01-2025")
    ultimo = (
        df.sort(["data_expiracao", "variacao_strike_bps"])
        .group_by("data_expiracao")
        .agg(pl.col("prob_acumulada").last())
    )
    assert (ultimo["prob_acumulada"] - 1.0).abs().max() < 1e-9


def test_raw_prob_non_negative(cpm_patchado):
    df = modulo_probabilidades.all_meetings("29-01-2025")
    assert (df["prob_bruta"] >= 0.0).all()


def test_prob_non_negative(cpm_patchado):
    df = modulo_probabilidades.all_meetings("29-01-2025")
    assert (df["prob"] >= 0.0).all()


def test_cum_prob_monotone(cpm_patchado):
    df = modulo_probabilidades.all_meetings("29-01-2025")
    for data_expiracao in df["data_expiracao"].unique().to_list():
        sub = df.filter(pl.col("data_expiracao") == data_expiracao).sort(
            "variacao_strike_bps"
        )
        diferencas = sub["prob_acumulada"].diff().drop_nulls()
        assert (diferencas >= -1e-12).all(), (
            f"prob_acumulada nao monotona para {data_expiracao}"
        )


# ── MeetingRank ───────────────────────────────────────────────────────────


def test_meeting_rank_starts_at_one(cpm_patchado):
    df = modulo_probabilidades.all_meetings("29-01-2025")
    assert df["ranking_reuniao"].min() == 1


def test_meeting_rank_consecutive(cpm_patchado):
    df = modulo_probabilidades.all_meetings("29-01-2025")
    rankings = df["ranking_reuniao"].unique().sort().to_list()
    assert rankings == list(range(1, len(rankings) + 1))


# ── Null-price meetings excluded ─────────────────────────────────────────


def test_null_price_meeting_excluded(monkeypatch, cpm_fixture):
    """A meeting where all strikes have null preco_ajuste is excluded."""
    reuniao_nula = cpm_fixture.with_columns(
        pl.when(pl.col("codigo_negociacao").str.starts_with("CPMK25"))
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col("preco_ajuste"))
        .alias("preco_ajuste")
    )
    monkeypatch.setattr(modulo_cpm, "data", lambda _: reuniao_nula)
    monkeypatch.setattr(
        modulo_probabilidades.di1,
        "interpolar_taxas",
        lambda *a, **kw: pl.Series("taxa_interpolada", [0.0] * len(a[0])),
    )
    df = modulo_probabilidades.all_meetings("29-01-2025")
    assert datetime.date(2025, 5, 8) not in df["data_expiracao"].to_list()
    assert df["ranking_reuniao"].min() == 1


# ── meeting() selection ───────────────────────────────────────────────────


def test_meeting_nearest_is_rank_one(cpm_patchado):
    df = modulo_probabilidades.meeting("29-01-2025")
    assert df["ranking_reuniao"].unique().to_list() == [1]


def test_meeting_nearest_single_expiry(cpm_patchado):
    df = modulo_probabilidades.meeting("29-01-2025")
    assert df["data_expiracao"].n_unique() == 1


def test_meeting_explicit_expiration(cpm_patchado):
    df_todas = modulo_probabilidades.all_meetings("29-01-2025")
    maior_ranking = df_todas["ranking_reuniao"].max()
    segunda_data_expiracao = df_todas.filter(
        pl.col("ranking_reuniao") == maior_ranking
    )["data_expiracao"][0]
    df = modulo_probabilidades.meeting("29-01-2025", expiration=segunda_data_expiracao)
    assert df["data_expiracao"].unique().item() == segunda_data_expiracao


def test_meeting_rank_always_one(cpm_patchado):
    df = modulo_probabilidades.meeting("29-01-2025")
    assert (df["ranking_reuniao"] == 1).all()


def test_meeting_prob_sums_to_one(cpm_patchado):
    df = modulo_probabilidades.meeting("29-01-2025")
    assert abs(df["prob"].sum() - 1.0) < 1e-9


# ── Spot checks ───────────────────────────────────────────────────────────


def test_nearest_meeting_expiry_date(cpm_patchado):
    df = modulo_probabilidades.meeting("29-01-2025")
    assert df["data_expiracao"].unique().item() == datetime.date(2025, 1, 30)


def test_highest_prob_strike_jan2025(cpm_patchado):
    """On 2025-01-29, +100 bps was the overwhelmingly dominant strike."""
    df = modulo_probabilidades.meeting("29-01-2025")
    maior_strike = df.sort("prob", descending=True)["variacao_strike_bps"][0]
    assert maior_strike == 100


def test_discount_exp_one_when_rate_zero(cpm_patchado):
    """With di1 patched to return 0.0, fator_desconto must equal 1.0."""
    df = modulo_probabilidades.all_meetings("29-01-2025")
    assert (df["fator_desconto"] - 1.0).abs().max() < 1e-12


def test_raw_prob_equals_settlement_over_100_when_rate_zero(cpm_patchado):
    """With fator_desconto=1.0, prob_bruta = preco_ajuste / 100."""
    df = modulo_probabilidades.all_meetings("29-01-2025")
    esperado = df["preco_ajuste"] / 100
    diferenca = (df["prob_bruta"] - esperado).abs().max()
    assert diferenca < 1e-12
