import datetime as dt
from pathlib import Path

import polars as pl

from pyield.anbima.imaq import imaq


def test_save_baseline():
    """Save baseline data for migration comparison."""
    baseline_date = dt.date(2026, 1, 29)
    df = imaq(baseline_date)

    baseline_path = Path(__file__).parent / "baseline_imaq_20260129.parquet"
    df.write_parquet(baseline_path)

    # Validations
    assert df.shape[0] > 0, "Should have data rows"
    assert df.shape[1] == 12, "Should have 12 columns"
    assert "BondType" in df.columns
    assert "Price" in df.columns


def test_migration_matches_baseline():
    """Verify migrated implementation matches baseline."""
    baseline_date = dt.date(2026, 1, 29)
    baseline_path = Path(__file__).parent / "baseline_imaq_20260129.parquet"

    baseline_df = pl.read_parquet(baseline_path)
    migrated_df = imaq(baseline_date)

    # Shape comparison
    assert migrated_df.shape == baseline_df.shape, (
        f"Shape mismatch: {migrated_df.shape} vs {baseline_df.shape}"
    )

    # Column comparison
    assert migrated_df.columns == baseline_df.columns

    # Data type comparison
    assert migrated_df.dtypes == baseline_df.dtypes

    # Value comparison
    assert migrated_df.equals(baseline_df), "DataFrames are not equal"
