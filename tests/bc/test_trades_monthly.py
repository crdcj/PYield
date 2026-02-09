from pathlib import Path

import polars as pl

import pyield.bc.trades_monthly as monthly_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_ZIP = DIRETORIO_DADOS / "trades_monthly_202501.zip"
CAMINHO_PARQUET = DIRETORIO_DADOS / "trades_monthly_202501.parquet"


def test_tpf_monthly_trades_com_monkeypatch(monkeypatch):
    """tpf_monthly_trades com monkeypatch deve bater com o parquet."""
    monkeypatch.setattr(monthly_mod, "_baixar_zip", lambda *_: CAMINHO_ZIP.read_bytes())
    resultado = monthly_mod.tpf_monthly_trades("07-01-2025", extragroup=True)
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
