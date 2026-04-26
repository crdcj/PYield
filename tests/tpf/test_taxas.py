import importlib
from pathlib import Path

import polars as pl

modulo_mercado_secundario = importlib.import_module("pyield.anbima.mercado_secundario")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "tpf_20260206.txt"
CAMINHO_PARQUET = DIRETORIO_DADOS / "tpf_20260206.parquet"


def test_pipeline_taxas():
    """tpf.taxas(): pipeline de processamento do CSV bruto deve bater com o parquet de referência."""
    csv_bruto = CAMINHO_CSV.read_bytes()
    df_resultado = modulo_mercado_secundario._parsear_df(csv_bruto)
    df_resultado = modulo_mercado_secundario._processar_df(df_resultado)

    assert df_resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
