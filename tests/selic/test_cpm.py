"""
Tests for pyield.selic.cpm — CPM contract data and ticker parsing.

Ticker parsing tests are pure unit tests (no I/O).
Data correctness tests use the fixture parquet.
"""

import datetime
from pathlib import Path

import polars as pl
import pytest

from pyield import dus
from pyield.selic.cpm import _empty_schema, _parse_ticker

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
    df = _empty_schema()
    assert df["data_referencia"].dtype == pl.Date
    assert df["preco_ajuste"].dtype == pl.Float64
    assert df["variacao_strike_bps"].dtype == pl.Int32
    assert df["dias_uteis"].dtype == pl.Int32


# ── Data correctness (fixture) ────────────────────────────────────────────


def test_settlement_price_range(cpm_fixture):
    non_null = cpm_fixture["preco_ajuste"].drop_nulls()
    assert (non_null >= 0.0).all()
    assert (non_null <= 100.0).all()


def test_option_type_values(cpm_fixture):
    assert cpm_fixture["tipo_opcao"].is_in(["call", "put"]).all()


def test_strike_multiples_of_25(cpm_fixture):
    assert (cpm_fixture["variacao_strike_bps"] % 25 == 0).all()


def test_meeting_end_before_expiry(cpm_fixture):
    non_null = cpm_fixture.filter(pl.col("data_fim_reuniao").is_not_null())
    assert (non_null["data_fim_reuniao"] < non_null["data_expiracao"]).all()


def test_meeting_end_date_not_null(cpm_fixture):
    assert cpm_fixture["data_fim_reuniao"].null_count() == 0


def test_expiry_is_one_bday_after_meeting_end(cpm_fixture):
    for row in cpm_fixture.iter_rows(named=True):
        expected = dus.deslocar(row["data_fim_reuniao"], 1)
        assert row["data_expiracao"] == expected


def test_bdays_to_exp_positive(cpm_fixture):
    assert (cpm_fixture["dias_uteis"] > 0).all()


# ── Spot checks (fixture: 2025-01-29) ────────────────────────────────────


def test_spot_cpmf25_expiry(cpm_fixture):
    row = cpm_fixture.filter(pl.col("codigo_negociacao").str.starts_with("CPMF25"))
    assert row["data_expiracao"].unique().item() == datetime.date(2025, 1, 30)


def test_spot_cpmf25_meeting_end(cpm_fixture):
    row = cpm_fixture.filter(pl.col("codigo_negociacao").str.starts_with("CPMF25"))
    assert row["data_fim_reuniao"].unique().item() == datetime.date(2025, 1, 29)


def test_spot_hold_strike_is_zero(cpm_fixture):
    hold = cpm_fixture.filter(pl.col("codigo_negociacao") == "CPMF25C100000")
    assert len(hold) == 1
    assert hold["variacao_strike_bps"].item() == 0


def test_spot_most_negative_strike(cpm_fixture):
    min_bps = cpm_fixture.filter(pl.col("codigo_negociacao").str.starts_with("CPMF25"))[
        "variacao_strike_bps"
    ].min()
    assert min_bps == -100


def test_spot_bdays_to_exp_cpmf25(cpm_fixture):
    bdays = (
        cpm_fixture.filter(pl.col("codigo_negociacao").str.starts_with("CPMF25"))[
            "dias_uteis"
        ]
        .unique()
        .item()
    )
    assert bdays == 1


def test_spot_bdays_to_exp_cpmk25(cpm_fixture):
    bdays = (
        cpm_fixture.filter(pl.col("codigo_negociacao").str.starts_with("CPMK25"))[
            "dias_uteis"
        ]
        .unique()
        .item()
    )
    assert bdays == 66
