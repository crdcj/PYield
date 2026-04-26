import datetime as dt
import importlib
from pathlib import Path

import polars as pl

modulo_imaq = importlib.import_module("pyield.anbima.imaq")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_HTML = DIRETORIO_DADOS / "imaq_20260204.html"
CAMINHO_PARQUET = DIRETORIO_DADOS / "imaq_20260204.parquet"
DATA_REFERENCIA = dt.date(2026, 2, 4)


def test_pipeline_estoque(monkeypatch):
    """tpf.estoque() com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        modulo_imaq,
        "_buscar_conteudo_url",
        lambda _: CAMINHO_HTML.read_bytes(),
    )
    resultado = modulo_imaq.estoque(data=DATA_REFERENCIA)
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
