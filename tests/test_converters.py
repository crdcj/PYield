import datetime as dt

import polars as pl
import pytest

from pyield.converters import convert_dates, validate_date_format

# ── validate_date_format ─────────────────────────────────────────────


def test_validate_brazilian_dash():
    assert validate_date_format("25-12-2024") == "%d-%m-%Y"


def test_validate_brazilian_slash():
    assert validate_date_format("25/12/2024") == "%d/%m/%Y"


def test_validate_iso():
    assert validate_date_format("2024-12-25") == "%Y-%m-%d"


def test_validate_invalid_format():
    with pytest.raises(ValueError, match="Formato de data inválido"):
        validate_date_format("12.25.2024")


def test_validate_invalid_nonsense():
    with pytest.raises(ValueError, match="Formato de data inválido"):
        validate_date_format("not-a-date")


# ── convert_dates: escalares ─────────────────────────────────────────


def test_scalar_brazilian_string():
    assert convert_dates("25-12-2024") == dt.date(2024, 12, 25)


def test_scalar_iso_string():
    assert convert_dates("2024-12-25") == dt.date(2024, 12, 25)


def test_scalar_date_passthrough():
    d = dt.date(2024, 12, 25)
    assert convert_dates(d) == d


def test_scalar_datetime_to_date():
    result = convert_dates(dt.datetime(2024, 12, 25, 14, 30))
    assert result == dt.date(2024, 12, 25)


def test_scalar_none():
    assert convert_dates(None) is None


def test_scalar_empty_string():
    """Regressão: string vazia não deve causar IndexError."""
    assert convert_dates("") is None


# ── convert_dates: coleções ──────────────────────────────────────────


def test_list_of_strings():
    result = convert_dates(["01-01-2024", "15-06-2024"])
    expected = pl.Series(values=[dt.date(2024, 1, 1), dt.date(2024, 6, 15)])
    assert result.equals(expected)


def test_list_with_none():
    result = convert_dates(["01-01-2024", None])
    expected_len = 2
    assert result.dtype == pl.Date
    assert result.len() == expected_len
    assert result.item(0) == dt.date(2024, 1, 1)
    assert result.item(1) is None


def test_all_empty_strings():
    """Regressão: lista de strings vazias não deve causar IndexError."""
    result = convert_dates(["", "  "])
    expected_len = 2
    assert result.dtype == pl.Date
    assert result.len() == expected_len
    assert result.null_count() == expected_len


def test_mixed_empty_and_valid():
    result = convert_dates(["", "25-12-2024", "  "])
    expected_len = 3
    assert result.dtype == pl.Date
    assert result.len() == expected_len
    assert result.item(1) == dt.date(2024, 12, 25)
    assert result.item(0) is None
    assert result.item(2) is None


def test_series_of_dates():
    s = pl.Series(values=[dt.date(2024, 1, 1), dt.date(2024, 6, 15)])
    result = convert_dates(s)
    assert result.equals(s)


def test_series_of_strings():
    s = pl.Series(values=["01-01-2024", "15-06-2024"])
    result = convert_dates(s)
    expected = pl.Series(values=[dt.date(2024, 1, 1), dt.date(2024, 6, 15)])
    assert result.equals(expected)


def test_mixed_formats_nullifies_mismatched():
    """Formatos mistos: primeiro formato detectado determina parsing;
    elementos incompatíveis viram null silenciosamente."""
    result = convert_dates(["25-12-2024", "2024-12-26"])
    assert result.dtype == pl.Date
    assert result.item(0) == dt.date(2024, 12, 25)
    assert result.item(1) is None
