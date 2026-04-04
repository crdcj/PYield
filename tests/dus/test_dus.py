# pyright: reportGeneralTypeIssues=false

import datetime as dt

import polars as pl

from pyield import dus


def test_count_new_holiday():
    start = "20-11-2024"  # Wednesday (Zumbi Nacional Day)
    end = "21-11-2024"
    expected_result = 0
    result = dus.contar(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_old_holiday():
    start = "20-11-2020"  # Friday (was not a holiday in 2020)
    end = "21-11-2020"
    expected_result = 1
    result = dus.contar(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_old_and_new_holidays_lists():
    start = ["20-11-2020", "20-11-2024"]
    end = ["21-11-2020", "21-11-2024"]
    expected_result = [1, 0]
    result = dus.contar(start, end)
    assert isinstance(result, pl.Series)
    assert result.to_list() == expected_result, (
        f"Expected {expected_result}, but got {result.to_list()}"
    )


def test_offset_with_old_holiday():
    start = "20-11-2020"
    offset = 0
    expected_result = dt.date(2020, 11, 20)
    result = dus.deslocar(start, offset)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_offset_with_new_holiday():
    start = "20-11-2024"
    offset = 0
    expected_result = dt.date(2024, 11, 21)
    result = dus.deslocar(start, offset)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_offset_with_old_and_new_holidays():
    start = ["20-11-2020", "20-11-2024"]
    offset = 0
    expected_result = [dt.date(2020, 11, 20), dt.date(2024, 11, 21)]
    result = dus.deslocar(start, offset)
    assert isinstance(result, pl.Series)
    # Polars Series of dates returns python date objects in to_list()
    assert result.to_list() == expected_result, (
        f"Expected {expected_result}, but got {result.to_list()}"
    )


def test_generate_invalid_start_falls_back_to_today(monkeypatch):
    data_fixa = dt.date(2024, 3, 1)
    monkeypatch.setattr("pyield.dus.core.relogio.hoje", lambda: data_fixa)

    result = dus.gerar(inicio="31-02-2024", fim="04-03-2024")
    expected = dus.gerar(inicio=None, fim="04-03-2024")

    assert result.equals(expected)


def test_generate_invalid_end_falls_back_to_today(monkeypatch):
    data_fixa = dt.date(2024, 3, 4)
    monkeypatch.setattr("pyield.dus.core.relogio.hoje", lambda: data_fixa)

    result = dus.gerar(inicio="01-03-2024", fim="31-02-2024")
    expected = dus.gerar(inicio="01-03-2024", fim=None)

    assert result.equals(expected)
