from pathlib import Path
from unittest.mock import patch

import polars as pl

from pyield.anbima.ettj_last import (
    _convert_csv_to_df,  # noqa: PLC2701
    _filter_ettf_text,  # noqa: PLC2701
    _get_reference_date,  # noqa: PLC2701
    _process_df,  # noqa: PLC2701
    last_ettj,
)

DATA_DIR = Path(__file__).parent / "data"
TXT_PATH = DATA_DIR / "ettj_last.txt"
PARQUET_PATH = DATA_DIR / "ettj_last.parquet"


def _load_text() -> str:
    return TXT_PATH.read_text(encoding="latin1")


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def test_process_pipeline():
    """Pipeline de filter+parse+process deve bater com o parquet de referência."""
    text = _load_text()
    reference_date = _get_reference_date(text)
    filtered = _filter_ettf_text(text)
    df = _convert_csv_to_df(filtered)
    result = _process_df(df, reference_date)
    assert result.equals(_load_expected())


def test_last_ettj_with_mock():
    """last_ettj com mock deve bater com o parquet de referência."""
    with patch(
        "pyield.anbima.ettj_last._get_last_content_text",
        return_value=_load_text(),
    ):
        result = last_ettj()
    assert result.equals(_load_expected())
