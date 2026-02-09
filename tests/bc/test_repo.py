from pathlib import Path

import polars as pl

import pyield.bc.repo as repo_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "repo_20250821.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "repo_20250821.parquet"


def test_repos_com_monkeypatch(monkeypatch):
    """repos com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        repo_mod,
        "_buscar_csv_api",
        lambda *_: CAMINHO_CSV.read_text(encoding="utf-8"),
    )
    resultado = repo_mod.repos(start="21-08-2025", end="21-08-2025")
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
