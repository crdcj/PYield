# arquivo: test_data_fetching.py

from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import pyield as yd
from pyield.fetchers.futures_data import xml as fx


def prepare_data(contract_code: str, trade_date: str) -> tuple:
    """Prepares the data for comparison."""
    file_date = pd.Timestamp(trade_date).strftime("%y%m%d")
    file_path = Path(f"./tests/data/SPRD{file_date}.zip")
    expected_df = fx.read_df(file_path=file_path, asset_code=contract_code)
    expected_df = expected_df.drop(columns=["AvgRate"])

    result_df = yd.futures(contract_code, trade_date)

    # Ensure that both DataFrames have the same columns
    expected_cols = set(expected_df.columns)
    result_cols = set(result_df.columns)
    common_cols = list(expected_cols.intersection(result_cols))
    result_df = result_df[common_cols].copy()
    expected_df = expected_df[common_cols].copy()

    # Ensure that the TickerSymbol is the same
    result_df.query("TickerSymbol in @expected_df.TickerSymbol", inplace=True)
    result_df.reset_index(drop=True, inplace=True)

    return result_df, expected_df


@pytest.mark.parametrize(
    ("asset_code", "reference_date"),
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
def test_fetch_and_prepare_data(asset_code, reference_date):
    """Tests if the asset data fetched matches the expected data read from file."""
    result_df, expected_df = prepare_data(asset_code, reference_date)
    assert_frame_equal(result_df, expected_df)
