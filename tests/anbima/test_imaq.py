import datetime as dt
from pathlib import Path
from unittest.mock import patch

import polars as pl

from pyield.anbima.imaq import imaq

DATA_DIR = Path(__file__).parent / "data"
HTML_PATH = DATA_DIR / "imaq_20260204.html"
TPF_PATH = DATA_DIR / "imaq_tpf_20260204.parquet"
PARQUET_PATH = DATA_DIR / "imaq_20260204.parquet"
REFERENCE_DATE = dt.date(2026, 2, 4)


def _load_html() -> bytes:
    return HTML_PATH.read_bytes()


def _load_tpf() -> pl.DataFrame:
    return pl.read_parquet(TPF_PATH)


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def test_imaq_with_mock():
    """imaq() with mocked fetches must match the reference parquet."""
    with (
        patch(
            "pyield.anbima.imaq._fetch_url_content",
            return_value=_load_html(),
        ),
        patch(
            "pyield.anbima.imaq.tpf_data",
            return_value=_load_tpf(),
        ),
    ):
        result = imaq(REFERENCE_DATE)
    assert result.equals(_load_expected())


def test_nullable_input_returns_empty():
    result = imaq(None)  # type: ignore[arg-type]
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()


def test_empty_html_returns_empty():
    with patch(
        "pyield.anbima.imaq._fetch_url_content",
        return_value=b"",
    ):
        result = imaq(REFERENCE_DATE)
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()
