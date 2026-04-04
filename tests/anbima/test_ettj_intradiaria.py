import importlib
from pathlib import Path

import polars as pl

ettj_mod = importlib.import_module("pyield.anbima.ettj_intradiaria")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "ettj_intradiaria.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ettj_intradiaria.parquet"


def test_ettj_intradiaria_com_monkeypatch(monkeypatch):
    """ettj_intradiaria com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        ettj_mod,
        "_buscar_texto_intraday",
        lambda: CAMINHO_CSV.read_text(encoding="latin1"),
    )
    resultado = ettj_mod.ettj_intradiaria()
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
