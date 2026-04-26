import importlib
from pathlib import Path

import polars as pl

modulo_tpf_intradia = importlib.import_module("pyield.bc.tpf_intradia")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "tpf_intradia_20260206.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "tpf_intradia_20260206.parquet"

# Colunas que dependem do horário de execução, não da lógica de processamento.
COLUNAS_IGNORAR = ["data_hora_consulta", "data_liquidacao"]


def test_pipeline_secundario_intradia(monkeypatch):
    """tpf.secundario_intradia() com monkeypatch deve bater com o parquet de referência."""

    def ler_csv() -> bytes:
        return CAMINHO_CSV.read_bytes()

    monkeypatch.setattr(modulo_tpf_intradia, "_buscar_csv", ler_csv)
    monkeypatch.setattr(modulo_tpf_intradia, "_mercado_selic_aberto", lambda: True)

    resultado = modulo_tpf_intradia.secundario_intradia().drop(COLUNAS_IGNORAR)
    esperado = pl.read_parquet(CAMINHO_PARQUET).drop(COLUNAS_IGNORAR)
    assert resultado.equals(esperado)
