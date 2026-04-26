import importlib
from pathlib import Path

import polars as pl

modulo_tpf_mensal = importlib.import_module("pyield.bc.tpf_mensal")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_ZIP = DIRETORIO_DADOS / "tpf_mensal_202501.zip"
CAMINHO_PARQUET = DIRETORIO_DADOS / "tpf_mensal_202501.parquet"


def test_tpf_mensal_com_monkeypatch(monkeypatch):
    """secundario_mensal_bcb com monkeypatch deve bater com o parquet bruto."""
    monkeypatch.setattr(
        modulo_tpf_mensal,
        "_baixar_zip",
        lambda *_: CAMINHO_ZIP.read_bytes(),
    )
    resultado = modulo_tpf_mensal.secundario_mensal_bcb("07-01-2025", extragrupo=True)
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
