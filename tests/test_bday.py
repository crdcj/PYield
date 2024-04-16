import numpy as np
import pandas as pd

import pyield as yd


def test_count_bdays_1():
    start = "2023-01-01"
    end = "2023-01-08"
    # 01/01/2023 is a Sunday and a holiday
    assert yd.count_bdays(start, end) == 5


def test_count_bdays_2():
    start = "2023-12-15"
    end = "2024-01-02"
    # 25/12/2023 is a holiday
    # 01/01/2024 is a holiday
    assert yd.count_bdays(start, end) == 10


def test_count_bdays_with_series():
    start = "2023-01-01"
    end = pd.Series(["2023-01-08", "2023-01-22"])
    # Assuming no holidays in these periods
    assert np.array_equal(yd.count_bdays(start, end), np.array([5, 15]))


def test_count_bdays_negative_count():
    start = "2023-01-08"
    end = "2023-01-01"
    # Negative count expected
    assert yd.count_bdays(start, end) == -5


def test_count_bdays_new_holiday_list():
    start = "2024-11-20"  # Zumbi Nacional Day
    end = "2024-11-21"
    assert yd.count_bdays(start, end) == 0


def test_count_bdays_old_holiday_list():
    start = "2020-11-20"  # Was not a holiday in 2020
    end = "2020-11-21"
    assert yd.count_bdays(start, end) == 1
