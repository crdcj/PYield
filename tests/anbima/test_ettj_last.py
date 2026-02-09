from pathlib import Path

import polars as pl

import pyield.anbima.ettj_last as ettj_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_TXT = DIRETORIO_DADOS / "ettj_last.txt"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ettj_last.parquet"


def test_last_ettj_com_monkeypatch(monkeypatch):
    """last_ettj com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        ettj_mod,
        "_buscar_texto_ultima_ettj",
        lambda: CAMINHO_TXT.read_text(encoding="latin1"),
    )
    resultado = ettj_mod.last_ettj()
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
