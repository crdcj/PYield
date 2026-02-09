import importlib
from pathlib import Path

import polars as pl

ettj_mod = importlib.import_module("pyield.anbima.ettj_intraday")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_TXT = DIRETORIO_DADOS / "ettj_intraday.txt"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ettj_intraday.parquet"


def test_intraday_ettj_com_monkeypatch(monkeypatch):
    """intraday_ettj com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        ettj_mod,
        "_fetch_intraday_text",
        lambda: CAMINHO_TXT.read_text(encoding="latin1"),
    )
    resultado = ettj_mod.intraday_ettj()
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
