from pathlib import Path

import polars as pl

import pyield.anbima.ima as ima_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "ima_completo.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ima_completo.parquet"


def test_last_ima_com_monkeypatch(monkeypatch):
    """last_ima com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        ima_mod,
        "_buscar_texto_ultimo_ima",
        lambda: CAMINHO_CSV.read_text(encoding="latin1"),
    )
    esperado = pl.read_parquet(CAMINHO_PARQUET).sort(
        "indice", "titulo", "data_vencimento"
    )
    resultado = ima_mod.last_ima()
    assert resultado.equals(esperado)
