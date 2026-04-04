import importlib
from pathlib import Path

import polars as pl

ima_mod = importlib.import_module("pyield.anbima.ima_ultimo")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_TXT = DIRETORIO_DADOS / "ima_ultimo.txt"
CAMINHO_PARQUET = DIRETORIO_DADOS / "ima_ultimo.parquet"


def test_ima_ultimo_com_monkeypatch(monkeypatch):
    """ima_ultimo com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        ima_mod,
        "_buscar_texto_ima_ultimo",
        lambda: CAMINHO_TXT.read_text(encoding="latin1"),
    )
    esperado = pl.read_parquet(CAMINHO_PARQUET).sort(
        "indice", "titulo", "data_vencimento"
    )
    resultado = ima_mod.ima_ultimo()
    assert resultado.equals(esperado)
