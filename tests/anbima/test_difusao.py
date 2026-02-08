import datetime as dt
from pathlib import Path
from unittest.mock import patch

import polars as pl

from pyield.anbima import difusao as df

DATA_DIR = Path(__file__).parent / "data"
CSV_PATH = DATA_DIR / "difusao_20260205.csv"
PARQUET_PATH = DATA_DIR / "difusao_20260205.parquet"
REFERENCE_DATE = dt.date(2026, 2, 5)


def _load_csv() -> str:
    return CSV_PATH.read_text(encoding="utf-8")


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def test_process_csv_data():
    """Processed CSV must match the saved reference parquet exactly."""
    result = df._process_csv_data(_load_csv())
    assert result.equals(_load_expected())


def test_tpf_difusao_with_mock():
    """tpf_difusao with mocked fetch must match the reference parquet."""
    with patch("pyield.anbima.difusao._fetch_url_data", return_value=_load_csv()):
        result = df.tpf_difusao(REFERENCE_DATE)
    assert result.equals(_load_expected())
