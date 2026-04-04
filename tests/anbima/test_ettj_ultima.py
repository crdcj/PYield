import importlib
from pathlib import Path

import polars as pl

modulo_ettj_ultima = importlib.import_module("pyield.anbima.ettj_ultima")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "ettj_ultima.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ettj_ultima.parquet"


def test_ettj_ultima_com_monkeypatch(monkeypatch):
    """ettj_ultima com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        modulo_ettj_ultima,
        "_buscar_texto_ettj_ultima",
        lambda: CAMINHO_CSV.read_text(encoding="latin1"),
    )
    resultado = modulo_ettj_ultima.ettj_ultima()
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
