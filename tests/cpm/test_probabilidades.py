"""Testes de yd.cpm.probabilidades.

Todos os testes fazem monkeypatch de contratos() e di1.interpolar_taxas()
via a fixture local cpm_patchado. Sem chamadas reais de rede.
"""

import datetime
from pathlib import Path

import polars as pl
import pytest

import pyield.cpm._contratos as modulo_contratos  # noqa: PLC2701
import pyield.cpm._probabilidades as modulo_probabilidades  # noqa: PLC2701

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

TOLERANCIA_PROB = 1e-9
TOLERANCIA_NUMERICA = 1e-12
STRIKE_DOMINANTE_JAN_2025 = 100


def _taxas_di1_zeradas(*, datas_referencia, **_kwargs) -> pl.Series:
    return pl.Series("taxa_interpolada", [0.0] * len(datas_referencia))


@pytest.fixture(scope="module")
def cpm_fixture() -> pl.DataFrame:
    df = pl.read_parquet(DIRETORIO_DADOS / "cpm_29012025.parquet")
    return df.rename(_RENOMEAR_COLUNAS, strict=False)


@pytest.fixture
def cpm_patchado(monkeypatch, cpm_fixture):
    monkeypatch.setattr(modulo_probabilidades, "contratos", lambda _data: cpm_fixture)
    monkeypatch.setattr(
        modulo_probabilidades.di1,
        "interpolar_taxas",
        _taxas_di1_zeradas,
    )
    return cpm_fixture


def _reuniao_mais_proxima() -> pl.DataFrame:
    df = modulo_probabilidades.probabilidades("29-01-2025")
    return df.filter(pl.col("ranking_reuniao") == 1)


# ── Empty schema ──────────────────────────────────────────────────────────


def test_empty_schema_zero_rows():
    df = modulo_probabilidades._df_vazio()
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
    assert modulo_probabilidades._df_vazio().columns == esperado


# ── Empty input propagation ───────────────────────────────────────────────


def test_probabilidades_empty_input(monkeypatch):
    monkeypatch.setattr(
        modulo_probabilidades, "contratos", lambda _: modulo_contratos._df_vazio()
    )
    resultado = modulo_probabilidades.probabilidades("01-01-2025")
    assert resultado.is_empty()
    assert resultado.columns == modulo_probabilidades._df_vazio().columns


# ── Schema of non-empty output ────────────────────────────────────────────


def test_probabilidades_schema(cpm_patchado):
    df = modulo_probabilidades.probabilidades("29-01-2025")
    assert df.columns == modulo_probabilidades._df_vazio().columns


# ── Probability invariants ────────────────────────────────────────────────


def test_prob_sums_to_one(cpm_patchado):
    df = modulo_probabilidades.probabilidades("29-01-2025")
    somas = df.group_by("data_expiracao").agg(pl.col("prob").sum())
    diferenca_maxima = (somas["prob"] - 1.0).abs().max()
    assert isinstance(diferenca_maxima, float)
    assert diferenca_maxima < TOLERANCIA_PROB


def test_cum_prob_ends_at_one(cpm_patchado):
    df = modulo_probabilidades.probabilidades("29-01-2025")
    ultimo = (
        df.sort("data_expiracao", "variacao_strike_bps")
        .group_by("data_expiracao")
        .agg(pl.col("prob_acumulada").last())
    )
    diferenca_maxima = (ultimo["prob_acumulada"] - 1.0).abs().max()
    assert isinstance(diferenca_maxima, float)
    assert diferenca_maxima < TOLERANCIA_PROB


def test_raw_prob_non_negative(cpm_patchado):
    df = modulo_probabilidades.probabilidades("29-01-2025")
    assert (df["prob_bruta"] >= 0.0).all()


def test_prob_non_negative(cpm_patchado):
    df = modulo_probabilidades.probabilidades("29-01-2025")
    assert (df["prob"] >= 0.0).all()


def test_cum_prob_monotone(cpm_patchado):
    df = modulo_probabilidades.probabilidades("29-01-2025")
    for data_expiracao in df["data_expiracao"].unique().to_list():
        sub = df.filter(pl.col("data_expiracao") == data_expiracao).sort(
            "variacao_strike_bps"
        )
        diferencas = sub["prob_acumulada"].diff().drop_nulls()
        assert (diferencas >= -TOLERANCIA_NUMERICA).all(), (
            f"prob_acumulada nao monotona para {data_expiracao}"
        )


# ── MeetingRank ───────────────────────────────────────────────────────────


def test_meeting_rank_starts_at_one(cpm_patchado):
    df = modulo_probabilidades.probabilidades("29-01-2025")
    assert df["ranking_reuniao"].min() == 1


def test_meeting_rank_consecutive(cpm_patchado):
    df = modulo_probabilidades.probabilidades("29-01-2025")
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
    monkeypatch.setattr(modulo_probabilidades, "contratos", lambda _: reuniao_nula)
    monkeypatch.setattr(
        modulo_probabilidades.di1,
        "interpolar_taxas",
        _taxas_di1_zeradas,
    )
    df = modulo_probabilidades.probabilidades("29-01-2025")
    assert datetime.date(2025, 5, 8) not in df["data_expiracao"].to_list()
    assert df["ranking_reuniao"].min() == 1


# ── Reunião mais próxima ──────────────────────────────────────────────────


def test_meeting_nearest_single_expiry(cpm_patchado):
    df = _reuniao_mais_proxima()
    assert df["data_expiracao"].n_unique() == 1


def test_meeting_prob_sums_to_one(cpm_patchado):
    df = _reuniao_mais_proxima()
    soma_prob = df["prob"].sum()
    assert isinstance(soma_prob, (int, float))
    assert abs(float(soma_prob) - 1.0) < TOLERANCIA_PROB


# ── Spot checks ───────────────────────────────────────────────────────────


def test_nearest_meeting_expiry_date(cpm_patchado):
    df = _reuniao_mais_proxima()
    assert df["data_expiracao"].unique().item() == datetime.date(2025, 1, 30)


def test_highest_prob_strike_jan2025(cpm_patchado):
    """On 2025-01-29, +100 bps was the overwhelmingly dominant strike."""
    df = _reuniao_mais_proxima()
    maior_strike = df.sort("prob", descending=True)["variacao_strike_bps"][0]
    assert maior_strike == STRIKE_DOMINANTE_JAN_2025


def test_discount_exp_one_when_rate_zero(cpm_patchado):
    """With di1 patched to return 0.0, fator_desconto must equal 1.0."""
    df = modulo_probabilidades.probabilidades("29-01-2025")
    diferenca_maxima = (df["fator_desconto"] - 1.0).abs().max()
    assert isinstance(diferenca_maxima, float)
    assert diferenca_maxima < TOLERANCIA_NUMERICA


def test_raw_prob_equals_settlement_over_100_when_rate_zero(cpm_patchado):
    """With fator_desconto=1.0, prob_bruta = preco_ajuste / 100."""
    df = modulo_probabilidades.probabilidades("29-01-2025")
    esperado = df["preco_ajuste"] / 100
    diferenca = (df["prob_bruta"] - esperado).abs().max()
    assert isinstance(diferenca, float)
    assert diferenca < TOLERANCIA_NUMERICA
