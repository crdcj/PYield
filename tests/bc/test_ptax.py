from pathlib import Path

import polars as pl

import pyield.bc.ptax_api as ptax_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "ptax_20250422-20250425.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ptax_20250422-20250425.parquet"


def test_ptax_series_com_monkeypatch(monkeypatch):
    """ptax_series com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        ptax_mod,
        "_buscar_texto_api",
        lambda *_: CAMINHO_CSV.read_bytes(),
    )
    resultado = ptax_mod.ptax_series(start="22-04-2025", end="25-04-2025")
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))


def test_ptax_com_monkeypatch(monkeypatch):
    """ptax com monkeypatch deve retornar o MidRate correto."""
    monkeypatch.setattr(
        ptax_mod,
        "_buscar_texto_api",
        lambda *_: CAMINHO_CSV.read_bytes(),
    )
    resultado = ptax_mod.ptax("22-04-2025")
    esperado = pl.read_parquet(CAMINHO_PARQUET)["MidRate"].item(0)
    assert resultado == esperado
