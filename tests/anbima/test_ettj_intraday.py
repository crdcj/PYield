from pathlib import Path
from unittest.mock import patch

import polars as pl

from pyield.anbima.ettj_intraday import (
    ROUND_DIGITS,
    _extract_date_and_tables,  # noqa: PLC2701
    _parse_intraday_table,  # noqa: PLC2701
    intraday_ettj,
)

DATA_DIR = Path(__file__).parent / "data"
TXT_PATH = DATA_DIR / "ettj_intraday.txt"
PARQUET_PATH = DATA_DIR / "ettj_intraday.parquet"


def _load_text() -> str:
    return TXT_PATH.read_text(encoding="latin1")


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def test_process_pipeline():
    """Pipeline de extract+parse+join deve bater com o parquet de referência."""
    text = _load_text()
    data_ref, tabela_pre, tabela_ipca = _extract_date_and_tables(text)

    df_pre = _parse_intraday_table(tabela_pre).rename({"D0": "nominal_rate"})
    df_ipca = _parse_intraday_table(tabela_ipca).rename({"D0": "real_rate"})

    df = df_pre.join(df_ipca, on="Vertices", how="right")
    df = df.rename({"Vertices": "vertex"})
    df = df.with_columns(
        pl.col("real_rate").truediv(100).round(ROUND_DIGITS),
        pl.col("nominal_rate").truediv(100).round(ROUND_DIGITS),
        date=data_ref,
    ).with_columns(
        ((pl.col("nominal_rate") + 1) / (pl.col("real_rate") + 1) - 1)
        .round(ROUND_DIGITS)
        .alias("implied_inflation"),
    )
    result = df.select(
        ["date", "vertex", "nominal_rate", "real_rate", "implied_inflation"]
    )
    assert result.equals(_load_expected())


def test_intraday_ettj_with_mock():
    """intraday_ettj com mock deve bater com o parquet de referência."""
    with patch(
        "pyield.anbima.ettj_intraday._fetch_intraday_text",
        return_value=_load_text(),
    ):
        result = intraday_ettj()
    assert result.equals(_load_expected())
