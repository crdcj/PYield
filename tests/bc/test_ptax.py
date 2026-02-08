from pathlib import Path
from unittest.mock import patch

import polars as pl

from pyield.bc.ptax_api import (
    _process_df,  # noqa: PLC2701
    _read_csv_data,  # noqa: PLC2701
    ptax,
    ptax_series,
)

DATA_DIR = Path(__file__).parent / "data"
CSV_PATH = DATA_DIR / "ptax_20250422-20250425.csv"
PARQUET_PATH = DATA_DIR / "ptax_20250422-20250425.parquet"


def _load_csv() -> bytes:
    return CSV_PATH.read_bytes()


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def _process_csv(csv_content: bytes) -> pl.DataFrame:
    df = _read_csv_data(csv_content)
    return _process_df(df)


def test_process_csv_data():
    """Processed CSV must match the saved reference parquet exactly."""
    result = _process_csv(_load_csv())
    assert result.equals(_load_expected())


def test_ptax_series_with_mock():
    """ptax_series() with mocked fetch must match the reference parquet."""
    with patch("pyield.bc.ptax_api._fetch_text_from_api", return_value=_load_csv()):
        result = ptax_series(start="22-04-2025", end="25-04-2025")
    assert result.equals(_load_expected())


def test_ptax_with_mock():
    """ptax() with mocked fetch must return the correct MidRate float."""
    with patch("pyield.bc.ptax_api._fetch_text_from_api", return_value=_load_csv()):
        result = ptax("22-04-2025")
    expected_mid = _load_expected()["MidRate"].item(0)
    assert result == expected_mid


def test_empty_csv_returns_empty():
    """Empty API response returns empty DataFrame."""
    with patch("pyield.bc.ptax_api._fetch_text_from_api", return_value=""):
        result = ptax_series(start="22-04-2025", end="25-04-2025")
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()
