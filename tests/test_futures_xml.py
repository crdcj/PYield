# arquivo: test_data_fetching.py

from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import pyield as yd
from pyield.futures import xml as fx


def prepare_data(asset_code: str, trade_date: str) -> tuple:
    """Prepares the data for comparison."""
    file_date = pd.Timestamp(trade_date).strftime("%y%m%d")
    file_path = Path(f"./tests/data/SPRD{file_date}.zip")
    expected_df = fx.read_df(file_path=file_path, asset_code=asset_code)
    expected_df = expected_df.drop(columns=["AvgRate"])

    result_df = yd.fetch_asset(asset_code=asset_code, reference_date=trade_date)
    result_df = result_df[expected_df.columns].copy()
    result_df.query("TickerSymbol in @expected_df.TickerSymbol", inplace=True)
    result_df.reset_index(drop=True, inplace=True)

    return result_df, expected_df


@pytest.mark.parametrize(
    "asset_code,reference_date",
    [
        ("DI1", "2024-04-26"),
        ("FRC", "2024-04-26"),
        ("DDI", "2024-04-26"),
        ("DAP", "2024-04-26"),
        ("DOL", "2024-04-26"),
        ("WDO", "2024-04-26"),
        ("IND", "2024-04-26"),
        ("WIN", "2024-04-26"),
    ],
)
def test_fetch_and_prepare_data(asset_code, reference_date):
    """Tests if the asset data fetched matches the expected data read from file."""
    result_df, expected_df = prepare_data(asset_code, reference_date)
    assert_frame_equal(result_df, expected_df)
