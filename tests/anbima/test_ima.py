from pathlib import Path

import polars as pl

import pyield.anbima.ima as ima_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_TXT = DIRETORIO_DADOS / "ima_completo.txt"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ima_completo.parquet"


def test_last_ima_com_monkeypatch(monkeypatch):
    """last_ima com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        ima_mod,
        "_fetch_last_ima_text",
        lambda: CAMINHO_TXT.read_text(encoding="latin1"),
    )
    esperado = pl.read_parquet(CAMINHO_PARQUET).sort("IMAType", "BondType", "Maturity")
    resultado = ima_mod.last_ima()
    assert resultado.equals(esperado)
