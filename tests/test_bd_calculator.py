import pytest
import pandas as pd
import numpy as np

from pyield import bd_calculator as bd


def test_convert_to_np_datetime_with_timestamp():
    pd_timestamp = pd.Timestamp("2023-01-01")
    expected = np.datetime64("2023-01-01", "D")
    assert bd.convert_to_np_datetime(pd_timestamp), expected


def test_convert_to_np_datetime_with_series():
    dates = pd.Series([pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")])
    expected = np.array(["2023-01-01", "2023-01-02"], dtype="datetime64[D]")
    assert np.array_equal(bd.convert_to_np_datetime(dates), expected)


def test_convert_to_np_datetime_with_invalid_type():
    with pytest.raises(TypeError):
        bd.convert_to_np_datetime("2023-01-01")


def test_count_business_days_with_timestamps1():
    start = pd.Timestamp("2023-01-01")
    end = pd.Timestamp("2023-01-08")
    # 01/01/2023 is a Sunday and a holiday
    assert bd.count_business_days(start, end) == 5


def test_count_business_days_with_timestamps2():
    start = pd.Timestamp("2023-12-15")
    end = pd.Timestamp("2024-01-02")
    # 25/12/2023 is a holiday
    # 01/01/2024 is a holiday
    assert bd.count_business_days(start, end) == 10


def test_count_business_days_with_series():
    start_series = pd.Series([pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-15")])
    end_series = pd.Series([pd.Timestamp("2023-01-08"), pd.Timestamp("2023-01-22")])
    # Assuming no holidays in these periods
    assert np.array_equal(
        bd.count_business_days(start_series, end_series), np.array([5, 5])
    )


def test_count_business_days_negative_count():
    start = pd.Timestamp("2023-01-08")
    end = pd.Timestamp("2023-01-01")
    # Negative count expected
    assert bd.count_business_days(start, end) == -5
