# pyright: reportGeneralTypeIssues=false

import datetime as dt

import polars as pl

from pyield import bday


def test_count_new_holiday():
    start = "20-11-2024"  # Wednesday (Zumbi Nacional Day)
    end = "21-11-2024"
    expected_result = 0
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_old_holiday():
    start = "20-11-2020"  # Friday (was not a holiday in 2020)
    end = "21-11-2020"
    expected_result = 1
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_old_and_new_holidays_lists():
    start = ["20-11-2020", "20-11-2024"]
    end = ["21-11-2020", "21-11-2024"]
    expected_result = [1, 0]
    result = bday.count(start, end)
    assert isinstance(result, pl.Series)
    assert result.to_list() == expected_result, (
        f"Expected {expected_result}, but got {result.to_list()}"
    )


def test_offset_with_old_holiday():
    start = "20-11-2020"
    offset = 0
    expected_result = dt.date(2020, 11, 20)
    result = bday.offset(start, offset)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_offset_with_new_holiday():
    start = "20-11-2024"
    offset = 0
    expected_result = dt.date(2024, 11, 21)
    result = bday.offset(start, offset)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_offset_with_old_and_new_holidays():
    start = ["20-11-2020", "20-11-2024"]
    offset = 0
    expected_result = [dt.date(2020, 11, 20), dt.date(2024, 11, 21)]
    result = bday.offset(start, offset)
    assert isinstance(result, pl.Series)
    # Polars Series of dates returns python date objects in to_list()
    assert result.to_list() == expected_result, (
        f"Expected {expected_result}, but got {result.to_list()}"
    )


def test_generate_invalid_start_falls_back_to_today(monkeypatch):
    data_fixa = dt.date(2024, 3, 1)
    monkeypatch.setattr("pyield.bday.core.clock.today", lambda: data_fixa)

    result = bday.generate(start="31-02-2024", end="04-03-2024")
    expected = bday.generate(start=None, end="04-03-2024")

    assert result.equals(expected)


def test_generate_invalid_end_falls_back_to_today(monkeypatch):
    data_fixa = dt.date(2024, 3, 4)
    monkeypatch.setattr("pyield.bday.core.clock.today", lambda: data_fixa)

    result = bday.generate(start="01-03-2024", end="31-02-2024")
    expected = bday.generate(start="01-03-2024", end=None)

    assert result.equals(expected)
