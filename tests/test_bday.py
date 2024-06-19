import numpy as np
import pandas as pd

import pyield as yd


def test_count_bdays_1():
    start = "01-01-2023"
    end = "08-01-2023"
    # 01/01/2023 is a Sunday and a holiday
    assert yd.bday.count(start, end) == 5


def test_count_bdays_2():
    start = "15-12-2023"
    end = "02-01-2024"
    # 25/12/2023 is a holiday
    # 01/01/2024 is a holiday
    assert yd.bday.count(start, end) == 10


def test_count_bdays_with_series():
    start = "01-01-2023"
    end = pd.Series(["08-01-2023", "22-01-2023"])
    # Assuming no holidays in these periods
    assert np.array_equal(yd.bday.count(start, end), np.array([5, 15]))


def test_count_bdays_negative_count():
    start = "08-01-2023"
    end = "01-01-2023"
    # Negative count expected
    assert yd.bday.count(start, end) == -5


def test_count_bdays_new_holiday_list():
    start = "20-11-2024"  # Zumbi Nacional Day
    end = "21-11-2024"
    assert yd.bday.count(start, end) == 0


def test_count_bdays_old_holiday_list():
    start = "20-11-2020"  # Was not a holiday in 2020
    end = "21-11-2020"
    assert yd.bday.count(start, end) == 1
