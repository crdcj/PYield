from pathlib import Path
from unittest.mock import patch

import polars as pl

from pyield.anbima.ima import _parse_df, _process_df, last_ima  # noqa: PLC2701

DATA_DIR = Path(__file__).parent / "data"
TXT_PATH = DATA_DIR / "ima_completo.txt"
PARQUET_PATH = DATA_DIR / "ima_completo.parquet"


def _load_text() -> bytes:
    return TXT_PATH.read_text(encoding="latin1").encode("utf-8")


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def test_process_pipeline():
    """Pipeline de parse+process deve bater com o parquet de referência."""
    text = _load_text()
    df = _parse_df(text)
    df = _process_df(df)
    assert df.equals(_load_expected())


def test_last_ima_with_mock():
    """last_ima com mock deve bater com o parquet de referência."""
    expected = _load_expected().sort("IMAType", "BondType", "Maturity")
    with patch("pyield.anbima.ima._fetch_last_ima_text", return_value=_load_text()):
        result = last_ima()
    assert result.equals(expected)


def test_last_ima_filter():
    """Filtro por ima_type deve retornar apenas o tipo solicitado."""
    with patch("pyield.anbima.ima._fetch_last_ima_text", return_value=_load_text()):
        result = last_ima("IMA-B")
    assert not result.is_empty()
    assert (result["IMAType"] == "IMA-B").all()


def test_last_ima_error_returns_empty():
    """Erro no fetch deve retornar DataFrame vazio."""
    with patch("pyield.anbima.ima._fetch_last_ima_text", side_effect=Exception("err")):
        result = last_ima()
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()
