from pathlib import Path
from unittest.mock import patch

import polars as pl

from pyield.bc.repo import (
    _ajustar_volume_zero,  # noqa: PLC2701
    _ler_csv,  # noqa: PLC2701
    _ordenar_selecionar_colunas,  # noqa: PLC2701
    _processar_df,  # noqa: PLC2701
    repos,
)

DATA_DIR = Path(__file__).parent / "data"
CSV_PATH = DATA_DIR / "repo_20250821.csv"
PARQUET_PATH = DATA_DIR / "repo_20250821.parquet"


def _load_csv() -> str:
    return CSV_PATH.read_text(encoding="utf-8")


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def _process_csv(csv_text: str) -> pl.DataFrame:
    df = _ler_csv(csv_text)
    df = _processar_df(df)
    df = _ajustar_volume_zero(df)
    return _ordenar_selecionar_colunas(df)


def test_process_csv_data():
    """Processed CSV must match the saved reference parquet exactly."""
    result = _process_csv(_load_csv())
    assert result.equals(_load_expected())


def test_repos_with_mock():
    """repos() with mocked fetch must match the reference parquet."""
    with patch("pyield.bc.repo._buscar_csv_api", return_value=_load_csv()):
        result = repos(start="21-08-2025", end="21-08-2025")
    assert result.equals(_load_expected())


def test_empty_csv_returns_empty():
    """Empty API response returns empty DataFrame."""
    with patch("pyield.bc.repo._buscar_csv_api", return_value=""):
        result = repos(start="21-08-2025", end="21-08-2025")
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()
