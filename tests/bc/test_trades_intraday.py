from pathlib import Path

import polars as pl

import pyield.bc.trades_intraday as trades_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "trades_intraday_20260206.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "trades_intraday_20260206.parquet"

# Colunas que dependem do horário de execução, não da lógica de processamento.
COLUNAS_IGNORAR = ["CollectedAt", "SettlementDate"]


def test_trades_intraday_com_monkeypatch(monkeypatch):
    """tpf_intraday_trades com monkeypatch deve bater com o parquet."""
    monkeypatch.setattr(
        trades_mod,
        "_buscar_csv",
        lambda: CAMINHO_CSV.read_text(encoding="utf-8"),
    )
    monkeypatch.setattr(trades_mod, "_mercado_selic_aberto", lambda: True)

    resultado = trades_mod.tpf_intraday_trades().drop(COLUNAS_IGNORAR)
    esperado = pl.read_parquet(CAMINHO_PARQUET).drop(COLUNAS_IGNORAR)
    assert resultado.equals(esperado)
