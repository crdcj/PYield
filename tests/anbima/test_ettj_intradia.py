import importlib
from pathlib import Path

import polars as pl

modulo_ettj_intradia = importlib.import_module("pyield.anbima.ettj_intradia")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "ettj_intradia.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ettj_intradia.parquet"


def test_ettj_intradia_com_monkeypatch(monkeypatch):
    """ettj_intradia com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        modulo_ettj_intradia,
        "_buscar_texto_intradia",
        lambda: CAMINHO_CSV.read_text(encoding="latin1"),
    )
    resultado = modulo_ettj_intradia.ettj_intradia()
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
