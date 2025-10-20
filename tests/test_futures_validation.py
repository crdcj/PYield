import datetime as dt

import pyield as yd


def test_future_date_returns_empty_df():
    tomorrow = dt.datetime.now().date() + dt.timedelta(days=1)
    df = yd.futures(contract_code="DI1", date=tomorrow.strftime("%d-%m-%Y"))
    assert df.is_empty(), "Expected empty DataFrame for future date"


def test_non_business_day_returns_empty_df():
    # Pick a Saturday (ensure it's Saturday)
    # Find next Saturday from today
    today = dt.datetime.now().date()
    days_ahead = (5 - today.weekday()) % 7  # 5 is Saturday (weekday Mon=0)
    saturday = today + dt.timedelta(days=days_ahead)
    df = yd.futures(contract_code="DI1", date=saturday.strftime("%d-%m-%Y"))
    assert df.is_empty(), "Expected empty DataFrame for weekend date"


def test_business_day_not_future_returns_data_or_empty_without_error():
    # Last business day should be valid; just ensure no validation block.
    last_bd = yd.bday.last_business_day()
    df = yd.futures(contract_code="DI1", date=last_bd.strftime("%d-%m-%Y"))
    # Can't guarantee data availability (depends on market time), so only assert type
    assert df is not None
