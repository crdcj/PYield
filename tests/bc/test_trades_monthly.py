from pathlib import Path
from unittest.mock import patch

import polars as pl

from pyield.bc.trades_monthly import (
    _process_df,  # noqa: PLC2701
    _read_dataframe_from_zip,  # noqa: PLC2701
    _uncompress_zip,  # noqa: PLC2701
    tpf_monthly_trades,
)

DATA_DIR = Path(__file__).parent / "data"
ZIP_PATH = DATA_DIR / "trades_monthly_202501.zip"
PARQUET_PATH = DATA_DIR / "trades_monthly_202501.parquet"


def _load_zip() -> bytes:
    return ZIP_PATH.read_bytes()


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def _process_zip(zip_content: bytes) -> pl.DataFrame:
    csv_content = _uncompress_zip(zip_content)
    df = _read_dataframe_from_zip(csv_content)
    return _process_df(df)


def test_process_pipeline():
    """ZIP local processado deve bater com o parquet de referência."""
    result = _process_zip(_load_zip())
    assert result.equals(_load_expected())


def test_tpf_monthly_trades_with_mock():
    """tpf_monthly_trades() com mock de _fetch_zip_from_url deve bater com o parquet."""
    with patch(
        "pyield.bc.trades_monthly._fetch_zip_from_url",
        return_value=_load_zip(),
    ):
        result = tpf_monthly_trades("07-01-2025", extragroup=True)
    assert result.equals(_load_expected())


def test_nullable_input_returns_empty():
    """Entrada None retorna DataFrame vazio."""
    result = tpf_monthly_trades(None)  # type: ignore[arg-type]
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()
