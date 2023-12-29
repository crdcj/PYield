import pandas as pd
import numpy as np

from pyield import br_calendar as bc


def test_count_business_days_1():
    start = pd.Timestamp("2023-01-01")
    end = pd.Timestamp("2023-01-08")
    # 01/01/2023 is a Sunday and a holiday
    assert bc.count_business_days(start, end) == 5


def test_count_business_days_2():
    start = pd.Timestamp("2023-12-15")
    end = pd.Timestamp("2024-01-02")
    # 25/12/2023 is a holiday
    # 01/01/2024 is a holiday
    assert bc.count_business_days(start, end) == 10


def test_count_business_days_with_series():
    start = pd.Timestamp("2023-01-01")
    end = pd.Series([pd.Timestamp("2023-01-08"), pd.Timestamp("2023-01-22")])
    # Assuming no holidays in these periods
    assert np.array_equal(bc.count_business_days(start, end), np.array([5, 15]))


def test_count_business_days_negative_count():
    start = pd.Timestamp("2023-01-08")
    end = pd.Timestamp("2023-01-01")
    # Negative count expected
    assert bc.count_business_days(start, end) == -5


def test_count_business_days_new_holiday_list():
    start = pd.Timestamp("2024-11-20")  # Zumbi Nacional Day
    end = pd.Timestamp("2024-11-21")
    assert bc.count_business_days(start, end) == 0


def test_count_business_days_old_holiday_list():
    start = pd.Timestamp("2020-11-20")  # Was not a holiday in 2020
    end = pd.Timestamp("2020-11-21")
    assert bc.count_business_days(start, end) == 1
