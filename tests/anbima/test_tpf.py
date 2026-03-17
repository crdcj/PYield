import datetime as dt
from pathlib import Path

import polars as pl

from pyield.anbima.tpf import _ler_csv, _processar_df_bruto

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "tpf_20260206.txt"
CAMINHO_PARQUET = DIRETORIO_DADOS / "tpf_20260206.parquet"
DATA_TESTE = dt.date(2026, 2, 6)


def test_process_pipeline():
    """Pipeline de processamento do CSV bruto deve bater com o parquet de referência."""
    csv_bruto = CAMINHO_CSV.read_bytes()
    df = _ler_csv(csv_bruto)
    df = _processar_df_bruto(df)

    assert df.equals(pl.read_parquet(CAMINHO_PARQUET))
