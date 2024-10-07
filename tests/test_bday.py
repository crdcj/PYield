import numpy as np
import pandas as pd

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
    end = pd.Series(["08-01-2023", "22-01-2023"])
    # Assuming no holidays in these periods
    expected_result = np.array([5, 15])
    result = bday.count(start, end)
    are_arrays_equal = np.array_equal(result, expected_result)
    assert are_arrays_equal, f"Expected {expected_result}, but got {result}"


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
    expected_result = pd.Series([1, 0])
    expected_result = expected_result.astype("Int64")
    result = bday.count(start, end)
    are_series_equal = result.equals(expected_result)
    assert are_series_equal, f"Expected {expected_result}, but got {result}"


def test_offset_with_old_holiday():
    start = "20-11-2020"
    offset = 0
    expected_result = pd.Timestamp("20-11-2020")
    result = bday.offset(start, offset)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_offset_with_new_holiday():
    start = "20-11-2024"
    offset = 0
    expected_result = pd.Timestamp("21-11-2024")
    result = bday.offset(start, offset)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_offset_with_old_and_new_holidays():
    start = ["20-11-2020", "20-11-2024"]
    offset = 0
    expected_result = pd.Series(["20-11-2020", "21-11-2024"], dtype="datetime64[ns]")
    result = bday.offset(start, offset)
    are_series_equal = result.equals(expected_result)
    assert are_series_equal, f"Expected {expected_result}, but got {result}"
