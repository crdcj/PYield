import importlib
from pathlib import Path

import polars as pl

monthly_mod = importlib.import_module("pyield.bc.tpf_mensal")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_ZIP = DIRETORIO_DADOS / "trades_monthly_202501.zip"
CAMINHO_PARQUET = DIRETORIO_DADOS / "trades_monthly_202501.parquet"


def test_tpf_mensal_com_monkeypatch(monkeypatch):
    """tpf_mensal com monkeypatch deve bater com o parquet bruto."""
    monkeypatch.setattr(monthly_mod, "_baixar_zip", lambda *_: CAMINHO_ZIP.read_bytes())
    resultado = monthly_mod.tpf_mensal("07-01-2025", extragrupo=True)
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
