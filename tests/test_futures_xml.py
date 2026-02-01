import datetime as dt
from pathlib import Path

import polars as pl
import pytest
import requests
from polars.testing import assert_frame_equal

from pyield import b3

# Configuração da release do GitHub
GITHUB_REPO = "crdcj/PYield"
RELEASE_TAG = "test-data-v1.0"
TEST_DATA_DIR = Path("./tests/data")


def download_test_data(file_name: str) -> Path:
    """Download test data file from GitHub release if not present locally."""
    local_path = TEST_DATA_DIR / file_name

    if local_path.exists():
        return local_path

    TEST_DATA_DIR.mkdir(parents=True, exist_ok=True)

    url = (
        f"https://github.com/{GITHUB_REPO}/releases/download/{RELEASE_TAG}/{file_name}"
    )

    try:
        response = requests.get(url)
        response.raise_for_status()
        local_path.write_bytes(response.content)
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to download {file_name}: {e}") from e

    return local_path


def prepare_data(
    date_str: str, contract_code: str
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Prepare Polars DataFrames for comparison."""
    day, month, year = date_str.split("-")
    file_name = f"PR{year[2:]}{month}{day}.zip"
    file_path = download_test_data(file_name)

    df_expect = b3.read_price_report(file_path=file_path, contract_code=contract_code)
    df_result = b3.futures(contract_code=contract_code, date=date_str)

    # A API da bmf pode ter valores inconsistentes em algumas datas.
    # A data de corte é 12-12-2025
    last_date_old_api = dt.datetime.strptime("12-12-2025", "%d-%m-%Y").date()
    ref_date = dt.datetime.strptime(date_str, "%d-%m-%Y").date()
    if ref_date <= last_date_old_api:
        # converter FinancialVolume para Int64 para evitar erros de comparação
        df_expect = df_expect.with_columns(pl.col("FinancialVolume").cast(pl.Int64))

    # Common columns
    common_cols = set(df_expect.columns) & set(df_result.columns)
    df_expect = df_expect.select(common_cols)
    df_result = df_result.select(common_cols)

    # Common Tickers
    common_tickers = set(df_expect["TickerSymbol"]) & set(df_result["TickerSymbol"])
    df_expect = df_expect.filter(pl.col("TickerSymbol").is_in(common_tickers))
    df_result = df_result.filter(pl.col("TickerSymbol").is_in(common_tickers))

    return df_result, df_expect


@pytest.mark.parametrize(
    ("date", "contract_code"),
    [
        ("02-02-2023", "DI1"),
        ("02-02-2023", "FRC"),
        ("02-02-2023", "DDI"),
        ("02-02-2023", "DAP"),
        ("02-02-2023", "DOL"),
        ("02-02-2023", "WDO"),
        ("02-02-2023", "IND"),
        ("02-02-2023", "WIN"),
        ("03-02-2025", "DI1"),
        ("03-02-2025", "FRC"),
        ("03-02-2025", "DDI"),
        ("03-02-2025", "DAP"),
        ("03-02-2025", "DOL"),
        ("03-02-2025", "WDO"),
        ("03-02-2025", "IND"),
        ("03-02-2025", "WIN"),
        ("12-01-2026", "DI1"),
        ("12-01-2026", "FRC"),
        ("12-01-2026", "DDI"),
        ("12-01-2026", "DAP"),
        ("12-01-2026", "DOL"),
        ("12-01-2026", "WDO"),
        ("12-01-2026", "IND"),
        ("12-01-2026", "WIN"),
    ],
)
def test_fetch_and_prepare_data(date, contract_code):
    """Tests if the asset data fetched matches the expected data read from file."""
    result_df, expect_df = prepare_data(date, contract_code)
    assert_frame_equal(
        result_df, expect_df, rel_tol=1e-4, check_exact=False, check_dtypes=True
    )
