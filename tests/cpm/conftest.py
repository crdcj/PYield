from pathlib import Path

import polars as pl
import pytest

import pyield.copom._calendario as calendario  # noqa: PLC2701

DADOS = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def cpm_fixture() -> pl.DataFrame:
    # Preserva o Parquet histórico em inglês; adapta apenas os nomes de coluna.
    return pl.read_parquet(DADOS / "cpm_29012025.parquet").rename(
        {
            "TradeDate": "data_referencia",
            "TickerSymbol": "codigo_negociacao",
            "MeetingEndDate": "data_fim_reuniao",
            "ExpiryDate": "data_expiracao",
            "OptionType": "tipo_opcao",
            "StrikeChangeBps": "variacao_strike_bps",
            "SettlementPrice": "preco_ajuste",
            "BDaysToExp": "dias_uteis",
        },
        strict=False,
    )


@pytest.fixture
def calendario_copom_local(monkeypatch):
    dados = DADOS.parents[1] / "copom" / "data"
    monkeypatch.setattr(calendario, "_buscar_atas", (dados / "atas.json").read_bytes)
    monkeypatch.setattr(
        calendario, "_buscar_calendario", (dados / "calendario.ics").read_bytes
    )
