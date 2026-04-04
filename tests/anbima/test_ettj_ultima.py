import importlib
from pathlib import Path

import polars as pl

ettj_mod = importlib.import_module("pyield.anbima.ettj_ultima")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "ettj_ultima.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ettj_ultima.parquet"


def test_ettj_ultima_com_monkeypatch(monkeypatch):
    """ettj_ultima com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        ettj_mod,
        "_buscar_texto_ettj_ultima",
        lambda: CAMINHO_CSV.read_text(encoding="latin1"),
    )
    resultado = ettj_mod.ettj_ultima()
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
