import datetime as dt

import polars as pl

from pyield import bday


def test_count_with_strings1():
    start = "01-01-2023"
    end = "08-01-2023"
    # 01/01/2023 is a Sunday and a holiday
    expected_result = 5
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_strings2():
    start = "15-12-2023"
    end = "02-01-2024"
    # 25/12/2023 is a holiday
    # 01/01/2024 is a holiday
    expected_result = 10
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_with_series():
    start = "01-01-2023"
    end = ["08-01-2023", "22-01-2023"]
    # Assuming no holidays in these periods
    expected_result = [5, 15]
    result = bday.count(start, end)
    assert isinstance(result, pl.Series)
    assert result.to_list() == expected_result, (
        f"Expected {expected_result}, but got {result.to_list()}"
    )


def test_count_negative_count():
    start = "08-01-2023"
    end = "01-01-2023"
    expected_result = -5  # Negative count expected
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


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
