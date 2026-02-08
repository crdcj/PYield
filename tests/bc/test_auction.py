import datetime as dt
from pathlib import Path
from unittest.mock import patch

import polars as pl
import polars.selectors as cs

import pyield.bc.auction as ac

DATA_DIR = Path(__file__).parent / "data"
CSV_PATH = DATA_DIR / "auction_20250819.csv"
PARQUET_PATH = DATA_DIR / "auction_20250819.parquet"

# PTAX do dia 2025-08-19 usada na geração do Parquet de referência
PTAX_DF = pl.DataFrame(
    {"Date": [dt.date(2025, 8, 19)], "PTAX": [5.4713]},
    schema={"Date": pl.Date, "PTAX": pl.Float64},
)


def _load_csv() -> bytes:
    return CSV_PATH.read_bytes()


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def _run_pipeline(csv_text: bytes, df_ptax: pl.DataFrame) -> pl.DataFrame:
    """Executa o pipeline de processamento completo sem acesso à rede."""
    df = ac._parse_csv(csv_text)
    df = ac._format_df(df)
    df = ac._process_df(df)
    df = ac._adjust_values_without_auction(df)
    df = ac._add_duration(df)
    df = ac._add_dv01(df)
    df = ac._add_usd_dv01(df, df_ptax)
    df = ac._add_avg_maturity(df)
    df = ac._sort_and_reorder_columns(df)
    df = df.with_columns(cs.float().fill_nan(None))
    return df


def test_process_pipeline():
    """Pipeline local deve produzir resultado idêntico ao Parquet de referência."""
    result = _run_pipeline(_load_csv(), PTAX_DF)
    assert result.equals(_load_expected())


def test_auctions_with_mock():
    """auctions() com mocks de rede deve produzir o Parquet de referência."""
    with (
        patch("pyield.bc.auction._get_api_csv", return_value=_load_csv()),
        patch("pyield.bc.auction._get_ptax_df", return_value=PTAX_DF),
    ):
        result = ac.auctions(start="19-08-2025", end="19-08-2025")
    assert result.equals(_load_expected())


def test_empty_csv_returns_empty():
    """Resposta vazia da API retorna DataFrame vazio."""
    with patch("pyield.bc.auction._get_api_csv", return_value=b""):
        result = ac.auctions(start="19-08-2025", end="19-08-2025")
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()
