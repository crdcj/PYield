from pathlib import Path

import polars as pl

from pyield.bc.trades_intraday import (
    _clean_csv,  # noqa: PLC2701
    _convert_csv_to_df,  # noqa: PLC2701
    _process_df,  # noqa: PLC2701
)

DATA_DIR = Path(__file__).parent / "data"
CSV_PATH = DATA_DIR / "trades_intraday_20260206.csv"
PARQUET_PATH = DATA_DIR / "trades_intraday_20260206.parquet"

# Columns that depend on execution time, not on data processing logic.
IGNORE_COLUMNS = ["CollectedAt", "SettlementDate"]


def _load_and_process() -> pl.DataFrame:
    csv_data = CSV_PATH.read_text(encoding="utf-8")
    cleaned = _clean_csv(csv_data)
    df = _convert_csv_to_df(cleaned)
    return _process_df(df).drop(IGNORE_COLUMNS)


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH).drop(IGNORE_COLUMNS)


def test_process_matches_reference():
    """Processed CSV must match the saved reference parquet exactly."""
    result = _load_and_process()
    assert result.equals(_load_expected())
