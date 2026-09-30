"""Testes de yd.cpm.contratos.

Os testes de corretude dos dados usam o parquet de referencia.
"""

import datetime
from pathlib import Path

import polars as pl
import pytest

import pyield.cpm._contratos as modulo_contratos  # noqa: PLC2701
from pyield import du

DIRETORIO_DADOS = Path(__file__).parent / "data"
PRECO_AJUSTE_MAXIMO = 100.0
STRIKE_MINIMO_CPMF25 = -100
DIAS_UTEIS_CPMK25 = 66

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


# ── Parsing do código de negociação ──────────────────────────────────────


@pytest.mark.parametrize(
    ("ticker", "mes_esperado", "ano_esperado", "tipo_esperado", "bps_esperado"),
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
def test_parse_ticker_valid(  # noqa: PLR0913, PLR0917
    monkeypatch,
    ticker,
    mes_esperado,
    ano_esperado,
    tipo_esperado,
    bps_esperado,
):
    monkeypatch.setattr(
        modulo_contratos.boletim,
        "buscar",
        lambda *args, **kwargs: pl.DataFrame({"codigo_negociacao": [ticker]}),
    )
    monkeypatch.setattr(
        modulo_contratos,
        "_buscar_csv",
        lambda data: "\ufeffInstrumento financeiro;Preço de referência\n".encode(),
    )
    linha = modulo_contratos.contratos("02-01-2025").row(0, named=True)
    assert linha["data_fim_reuniao"].month == mes_esperado
    assert linha["data_fim_reuniao"].year == ano_esperado
    assert linha["tipo_opcao"] == tipo_esperado
    assert linha["variacao_strike_bps"] == bps_esperado


# ── Schema vazio ──────────────────────────────────────────────────────────


def test_empty_schema_zero_rows():
    df = modulo_contratos._df_vazio()
    assert len(df) == 0


def test_empty_schema_columns():
    df = modulo_contratos._df_vazio()
    assert df.columns == [
        "data_referencia",
        "codigo_negociacao",
        "data_fim_reuniao",
        "data_expiracao",
        "tipo_opcao",
        "variacao_strike_bps",
        "preco_ajuste",
        "dias_uteis",
    ]


def test_empty_schema_dtypes():
    df = modulo_contratos._df_vazio()
    assert df["data_referencia"].dtype == pl.Date
    assert df["preco_ajuste"].dtype == pl.Float64
    assert df["variacao_strike_bps"].dtype == pl.Int32
    assert df["dias_uteis"].dtype == pl.Int32


# ── Corretude dos dados (fixture) ────────────────────────────────────────


def test_settlement_price_range(cpm_fixture):
    nao_nulos = cpm_fixture["preco_ajuste"].drop_nulls()
    assert (nao_nulos >= 0.0).all()
    assert (nao_nulos <= PRECO_AJUSTE_MAXIMO).all()


def test_option_type_values(cpm_fixture):
    assert cpm_fixture["tipo_opcao"].is_in(["call", "put"]).all()


def test_strike_multiples_of_25(cpm_fixture):
    assert (cpm_fixture["variacao_strike_bps"] % 25 == 0).all()


def test_meeting_end_before_expiry(cpm_fixture):
    nao_nulos = cpm_fixture.filter(pl.col("data_fim_reuniao").is_not_null())
    assert (nao_nulos["data_fim_reuniao"] < nao_nulos["data_expiracao"]).all()


def test_meeting_end_date_not_null(cpm_fixture):
    assert cpm_fixture["data_fim_reuniao"].null_count() == 0


def test_expiry_is_one_bday_after_meeting_end(cpm_fixture):
    for row in cpm_fixture.iter_rows(named=True):
        esperado = du.deslocar(row["data_fim_reuniao"], 1)
        assert row["data_expiracao"] == esperado


def test_bdays_to_exp_positive(cpm_fixture):
    assert (cpm_fixture["dias_uteis"] > 0).all()


# ── Checagens pontuais (fixture: 2025-01-29) ─────────────────────────────


def test_spot_cpmf25_meeting_end(cpm_fixture):
    row = cpm_fixture.filter(pl.col("codigo_negociacao").str.starts_with("CPMF25"))
    assert row["data_fim_reuniao"].unique().item() == datetime.date(2025, 1, 29)


def test_spot_hold_strike_is_zero(cpm_fixture):
    linha = cpm_fixture.filter(pl.col("codigo_negociacao") == "CPMF25C100000")
    assert len(linha) == 1
    assert linha["variacao_strike_bps"].item() == 0


def test_spot_most_negative_strike(cpm_fixture):
    min_bps = cpm_fixture.filter(pl.col("codigo_negociacao").str.starts_with("CPMF25"))[
        "variacao_strike_bps"
    ].min()
    assert min_bps == STRIKE_MINIMO_CPMF25


def test_spot_bdays_to_exp_cpmf25(cpm_fixture):
    dias_uteis = (
        cpm_fixture.filter(pl.col("codigo_negociacao").str.starts_with("CPMF25"))[
            "dias_uteis"
        ]
        .unique()
        .item()
    )
    assert dias_uteis == 1


def test_spot_bdays_to_exp_cpmk25(cpm_fixture):
    dias_uteis = (
        cpm_fixture.filter(pl.col("codigo_negociacao").str.starts_with("CPMK25"))[
            "dias_uteis"
        ]
        .unique()
        .item()
    )
    assert dias_uteis == DIAS_UTEIS_CPMK25
