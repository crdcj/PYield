import pandas as pd
import pytest

from pyield import futures as ft


def test_valid_old_contract_code1():
    expiration_code = "JAN3"  # Valid contract code
    trade_date = pd.Timestamp("2001-05-21")
    result = ft.get_old_expiration_date(expiration_code, trade_date)
    contract_expiration = pd.Timestamp("2003-01-02")
    assert result == contract_expiration


def test_valid_old_contract_code2():
    expiration_code = "JAN3"  # Valid contract code
    trade_date = pd.Timestamp("1990-01-01")
    result = ft.get_old_expiration_date(expiration_code, trade_date)
    contract_expiration = pd.Timestamp("1993-01-04")
    assert result == contract_expiration


def test_invalid_old_contract_code():
    expiration_code = "J3"  # Invalid contract code
    trade_date = pd.Timestamp("2001-01-02")
    # Must return NaT
    result = ft.get_old_expiration_date(expiration_code, trade_date)
    assert pd.isnull(result)


def test_new_contract_code():
    expiration_code = "F23"  # Valid contract code
    result = ft.get_expiration_date(expiration_code)
    contract_expiration = pd.Timestamp("2023-01-02")
    assert result == contract_expiration


def test_settlement_rate_with_old_holiday_list():
    settlement_rates = {
        "DI1N27": 0.09809,
        "DI1F33": 0.10368,
    }

    # 22-12-2023 is before the new holiday calendar
    test_date = pd.Timestamp("2023-12-22")
    df = ft.fetch_past_di(trade_date=test_date)
    tickers = list(settlement_rates.keys())  # noqa: F841
    result = df.query("TickerSymbol in @tickers")["SettlementRate"].to_list()
    assert result == list(settlement_rates.values())


def test_settlement_rates_with_current_holiday_list():
    settlement_rates = {
        "DI1F24": 0.11644,
        "DI1J24": 0.11300,
        "DI1N24": 0.10786,
        "DI1V24": 0.10321,
        "DI1F25": 0.10031,
        "DI1J25": 0.09852,
        "DI1N25": 0.09715,
        "DI1V25": 0.09651,
        "DI1F26": 0.09583,
        "DI1N26": 0.09631,
        "DI1F27": 0.09683,
        "DI1N27": 0.09794,
        "DI1F29": 0.10042,
        "DI1F31": 0.10240,
        "DI1F33": 0.10331,
    }
    test_date = pd.Timestamp("2023-12-26")
    df = ft.fetch_past_di(trade_date=test_date)
    tickers = list(settlement_rates.keys())  # noqa: F841
    results = df.query("TickerSymbol in @tickers")["SettlementRate"].to_list()
    assert results == list(settlement_rates.values())


def test_non_business_day():
    non_business_day = pd.Timestamp("2023-12-24")
    with pytest.raises(ValueError):
        ft.fetch_past_di(trade_date=non_business_day)
