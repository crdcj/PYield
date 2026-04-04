from pathlib import Path

import polars as pl

import pyield.bc.compromissada as repo_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "repo_20250821.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "repo_20250821.parquet"


def test_repos_com_monkeypatch(monkeypatch):
    """compromissadas com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        repo_mod,
        "_buscar_csv_api",
        lambda *_: CAMINHO_CSV.read_bytes(),
    )
    resultado = repo_mod.compromissadas(
        data_inicial="21-08-2025",
        data_final="21-08-2025",
    )
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
