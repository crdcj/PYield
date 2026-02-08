import datetime as dt
from pathlib import Path

import polars as pl
import polars.selectors as cs

from pyield.anbima import tpf

DATA_DIR = Path(__file__).parent / "data"
CSV_PATH = DATA_DIR / "tpf_20260206.txt"
PARQUET_PATH = DATA_DIR / "tpf_20260206.parquet"
DATA_TESTE = dt.date(2026, 2, 6)


def _load_csv() -> bytes:
    return CSV_PATH.read_bytes()


def _processar_csv() -> pl.DataFrame:
    df = tpf._ler_csv(_load_csv())
    df = tpf._processar_df_bruto(df)
    df = tpf._adicionar_duracao(df)
    df = tpf._adicionar_dv01(df, DATA_TESTE)
    df = tpf._adicionar_taxa_di(df, DATA_TESTE)
    df = tpf._selecionar_e_ordenar_colunas(df)
    # Substituir eventuais NaNs por None para compatibilidade com bancos de dados
    df = df.with_columns(cs.float().fill_nan(None))

    return df


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def test_process_pipeline():
    """Pipeline de processamento deve bater com o parquet de referência."""
    df = _processar_csv()
    assert df.equals(_load_expected())
