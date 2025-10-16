from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import pyield as yd
from pyield.b3.futures import ContractOptions
from pyield.b3.futures import xml as fx


def prepare_data(
    contract_code: ContractOptions,
    date: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Prepare Polars DataFrames for comparison eliminating pandas usage.

    Steps:
    - Build file path from provided date string (DD-MM-YYYY)
    - Read XML report (already returns Polars)
    - Clean and normalize specific columns
    - Fetch futures data (Polars)
    - Align columns and filter to expected tickers only
    """
    # Date parsing manually to avoid pandas dependency
    day, month, year = date.split("-")
    file_date = f"{year[2:]}{month}{day}"  # YYMMDD
    file_path = Path(f"./tests/data/SPRD{file_date}.zip")

    expected_df = fx.read_xml_report(file_path=file_path, contract_code=contract_code)

    # Round FinancialVolume to integer (keep as Int64 if present)
    if "FinancialVolume" in expected_df.columns:
        expected_df = expected_df.with_columns(
            pl.col("FinancialVolume").round(0).cast(pl.Int64)
        )

    # Drop variable rate columns if present
    drop_cols = [c for c in ["AvgRate", "ForwardRate"] if c in expected_df.columns]
    if drop_cols:
        expected_df = expected_df.drop(drop_cols)

    result_df = yd.futures(contract_code=contract_code, date=date)

    # Ensure both DataFrames have same columns
    common_cols = list(set(expected_df.columns) & set(result_df.columns))
    expected_df = expected_df.select(common_cols)
    result_df = result_df.select(common_cols)

    # Filter result_df to expected tickers (XML might have fewer tickers)
    if "TickerSymbol" in expected_df.columns:
        expected_tickers = expected_df.get_column("TickerSymbol")
        # Use implode to avoid deprecation warning on is_in ambiguity
        result_df = result_df.filter(
            pl.col("TickerSymbol").is_in(expected_tickers.implode())
        )

    return result_df, expected_df


@pytest.mark.parametrize(
    ("asset_code", "date"),
    [
        ("DI1", "22-12-2023"),
        ("FRC", "22-12-2023"),
        ("DDI", "22-12-2023"),
        ("DAP", "22-12-2023"),
        ("DOL", "22-12-2023"),
        ("WDO", "22-12-2023"),
        ("IND", "22-12-2023"),
        ("WIN", "22-12-2023"),
        ("DI1", "26-04-2024"),
        ("FRC", "26-04-2024"),
        ("DDI", "26-04-2024"),
        ("DAP", "26-04-2024"),
        ("DOL", "26-04-2024"),
        ("WDO", "26-04-2024"),
        ("IND", "26-04-2024"),
        ("WIN", "26-04-2024"),
    ],
)
def test_fetch_and_prepare_data(asset_code, date):
    """Tests if the asset data fetched matches the expected data read from file."""
    result_df, expected_df = prepare_data(asset_code, date)
    assert_frame_equal(
        result_df, expected_df, rel_tol=1e-4, check_exact=False, check_dtypes=True
    )
