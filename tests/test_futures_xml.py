from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import pyield as yd
from pyield.b3.futures import ContractOptions
from pyield.b3.futures import xml as fx


def prepare_data(
    contract_code: ContractOptions,
    date: str,
) -> tuple:
    """Prepares the data for comparison."""
    converted_date = pd.to_datetime(date, format="%d-%m-%Y")
    file_date = converted_date.strftime("%y%m%d")
    file_path = Path(f"./tests/data/SPRD{file_date}.zip")

    expected_df = fx.read_xml_report(file_path=file_path, contract_code=contract_code)
    for col in expected_df.columns:
        if col == "FinancialVolume":
            expected_df[col] = expected_df[col].round(0).astype("Int64[pyarrow]")

    # AvgRate can have different values in the XML file
    # and in the B3 website. We drop it to avoid comparison issues.
    # if "AvgRate" in expected_df.columns:
    #     expected_df = expected_df.drop(columns=["AvgRate"])
    for col in ["AvgRate", "ForwardRate"]:
        if col in expected_df.columns:
            expected_df = expected_df.drop(columns=[col])

    result_df = yd.futures(contract_code=contract_code, date=date)

    # Ensure that both DataFrames have the same columns
    expected_cols = set(expected_df.columns)
    result_cols = set(result_df.columns)
    common_cols = list(expected_cols & result_cols)
    result_df = result_df[common_cols].copy()
    expected_df = expected_df[common_cols].copy()

    # XML files can have less tickers than the B3 website (in zero open contracts)
    result_df = result_df.query(
        "TickerSymbol in @expected_df.TickerSymbol"
    ).reset_index(drop=True)

    result_df = pl.from_pandas(result_df)
    expected_df = pl.from_pandas(expected_df)

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
