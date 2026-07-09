import importlib
from pathlib import Path

import polars as pl
import pytest

modulo_secundario = importlib.import_module("pyield.tpf.secundario")
modulo_mensal = importlib.import_module("pyield.tpf.secundario._mensal")

DIRETORIO_DADOS = Path(__file__).parent / "data"
FIXTURES_MENSAIS = [
    ("07-06-2003", False, "tpf_mensal_200306"),
    ("07-01-2025", True, "tpf_mensal_202501"),
    ("07-06-2026", False, "tpf_mensal_202606"),
]
NOMES_FIXTURES_MENSAIS = [nome for _, _, nome in FIXTURES_MENSAIS]


def test_nome_arquivo_mensal():
    assert modulo_secundario.nome_arquivo_mensal("07-06-2026") == "NegT202606.ZIP"
    assert (
        modulo_secundario.nome_arquivo_mensal("07-01-2025", extragrupo=True)
        == "NegE202501.ZIP"
    )


@pytest.mark.parametrize(("data", "extragrupo", "nome"), FIXTURES_MENSAIS)
def test_pipeline_secundario_mensal(monkeypatch, data, extragrupo, nome):
    """tpf.secundario.mensal() com monkeypatch deve bater com o parquet."""
    caminho_zip = DIRETORIO_DADOS / f"{nome}.zip"
    caminho_parquet = DIRETORIO_DADOS / f"{nome}.parquet"
    monkeypatch.setattr(
        modulo_mensal,
        "baixar_zip",
        lambda *_: caminho_zip.read_bytes(),
    )
    resultado = modulo_secundario.mensal(data, extragrupo=extragrupo)
    assert "financeiro" in resultado.columns
    assert resultado.equals(pl.read_parquet(caminho_parquet))


@pytest.mark.parametrize("nome", NOMES_FIXTURES_MENSAIS)
def test_zip_para_silver_secundario_mensal(nome):
    """tpf.secundario.zip_para_silver() deve bater com o parquet de referência."""
    caminho_zip = DIRETORIO_DADOS / f"{nome}.zip"
    caminho_parquet = DIRETORIO_DADOS / f"{nome}.parquet"
    resultado = modulo_secundario.zip_para_silver(caminho_zip.read_bytes())
    esperado = pl.read_parquet(caminho_parquet).drop("financeiro")
    assert "financeiro" not in resultado.columns
    assert resultado.equals(esperado)


def test_baixar_zip_valida_conteudo(monkeypatch):
    """tpf.secundario.baixar_zip() não deve retornar bytes inválidos."""
    monkeypatch.setattr(modulo_mensal, "_baixar_url_zip", lambda *_: b"")

    with pytest.raises(ValueError, match="NegT202501.ZIP: ZIP inválido"):
        modulo_secundario.baixar_zip("07-01-2025")
