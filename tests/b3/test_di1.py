from pathlib import Path

import polars as pl

from pyield.b3 import di1 as di1_mod

TEST_DATA_DIR = Path(__file__).parent / "data"
CAMINHO_PARQUET = TEST_DATA_DIR / "futures_20260112_DI1.parquet"


def test_di1_data_compara_com_parquet_referencia(monkeypatch):
    """di1.data() deve bater com o parquet canônico de um dia."""
    esperado = pl.read_parquet(CAMINHO_PARQUET)

    monkeypatch.setattr(di1_mod, "_carregar_cache_pr_di1", lambda datas: esperado)
    monkeypatch.setattr(di1_mod.b3, "futures", lambda *args, **kwargs: pl.DataFrame())

    resultado = di1_mod.data(dates="12-01-2026")

    assert resultado.equals(esperado)
