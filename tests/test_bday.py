import numpy as np
import pandas as pd

from pyield import bday


def test_count_bdays_1():
    start = "01-01-2023"
    end = "08-01-2023"
    # 01/01/2023 is a Sunday and a holiday
    expected_result = 5
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_bdays_2():
    start = "15-12-2023"
    end = "02-01-2024"
    # 25/12/2023 is a holiday
    # 01/01/2024 is a holiday
    expected_result = 10
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_bdays_with_series():
    start = "01-01-2023"
    end = pd.Series(["08-01-2023", "22-01-2023"])
    # Assuming no holidays in these periods
    expected_result = np.array([5, 15])
    result = bday.count(start, end)
    are_arrays_equal = np.array_equal(result, expected_result)
    assert are_arrays_equal, f"Expected {expected_result}, but got {result}"


def test_count_bdays_negative_count():
    start = "08-01-2023"
    end = "01-01-2023"
    expected_result = -5  # Negative count expected
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_bdays_new_holiday_list():
    start = "20-11-2024"  # Zumbi Nacional Day
    end = "21-11-2024"
    expected_result = 0
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_bdays_old_holiday_list():
    start = "20-11-2020"  # Was not a holiday in 2020
    end = "21-11-2020"
    expected_result = 1
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"
