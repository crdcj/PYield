import importlib
from pathlib import Path

import polars as pl
import pytest

modulo_secundario = importlib.import_module("pyield.tpf.secundario")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_ZIP = DIRETORIO_DADOS / "tpf_mensal_202501.zip"
CAMINHO_PARQUET = DIRETORIO_DADOS / "tpf_mensal_202501.parquet"


def test_pipeline_secundario_mensal(monkeypatch):
    """tpf.secundario.mensal() com monkeypatch deve bater com o parquet."""
    monkeypatch.setattr(
        modulo_secundario,
        "baixar_zip",
        lambda *_: CAMINHO_ZIP.read_bytes(),
    )
    resultado = modulo_secundario.mensal("07-01-2025", extragrupo=True)
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))


def test_zip_para_silver_secundario_mensal():
    """tpf.secundario.zip_para_silver() deve bater com o parquet de referência."""
    resultado = modulo_secundario.zip_para_silver(CAMINHO_ZIP.read_bytes())
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))


def test_baixar_zip_valida_conteudo(monkeypatch):
    """tpf.secundario.baixar_zip() não deve retornar bytes inválidos."""
    monkeypatch.setattr(modulo_secundario, "_baixar_url_zip", lambda *_: b"")

    with pytest.raises(ValueError, match="NegT202501.ZIP: ZIP inválido"):
        modulo_secundario.baixar_zip("07-01-2025")
