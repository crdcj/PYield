import pandas as pd
import pytest

from pyield.di import core as cr
from pyield.di import web as web


def test_valid_old_contract_code1():
    expiration_code = "JAN3"  # Valid contract code
    trade_date = pd.Timestamp("2001-05-21")
    result = web.get_old_expiration_date(expiration_code, trade_date)
    contract_expiration = pd.Timestamp("2003-01-02")
    assert result == contract_expiration


def test_valid_old_contract_code2():
    expiration_code = "JAN3"  # Valid contract code
    trade_date = pd.Timestamp("1990-01-01")
    result = web.get_old_expiration_date(expiration_code, trade_date)
    contract_expiration = pd.Timestamp("1993-01-04")
    assert result == contract_expiration


def test_invalid_old_contract_code():
    expiration_code = "J3"  # Invalid contract code
    trade_date = pd.Timestamp("2001-01-02")
    # Must return NaT
    result = web.get_old_expiration_date(expiration_code, trade_date)
    assert pd.isnull(result)


def test_new_contract_code():
    expiration_code = "F23"  # Valid contract code
    result = cr.get_expiration_date(expiration_code)
    contract_expiration = pd.Timestamp("2023-01-02")
    assert result == contract_expiration


def test_settlement_rate_with_old_holiday_list():
    settlement_rates = {
        "N27": 0.09809,
        "F33": 0.10368,
    }

    # 22-12-2023 is before the new holiday calendar
    df = cr.get_di(trade_date="2023-12-22")
    expiration_codes = list(settlement_rates.keys())  # noqa: F841
    result = df.query("ExpirationCode in @expiration_codes")["SettlementRate"].to_list()
    assert result == list(settlement_rates.values())


def test_settlement_rates_with_current_holiday_list():
    settlement_rates = {
        "F24": 0.11644,
        "J24": 0.11300,
        "N24": 0.10786,
        "V24": 0.10321,
        "F25": 0.10031,
        "J25": 0.09852,
        "N25": 0.09715,
        "V25": 0.09651,
        "F26": 0.09583,
        "N26": 0.09631,
        "F27": 0.09683,
        "N27": 0.09794,
        "F29": 0.10042,
        "F31": 0.10240,
        "F33": 0.10331,
    }

    df = cr.get_di(trade_date="2023-12-26")
    expiration_codes = list(settlement_rates.keys())  # noqa: F841
    results = df.query("ExpirationCode in @expiration_codes")[
        "SettlementRate"
    ].to_list()
    assert results == list(settlement_rates.values())


def test_non_business_day():
    non_business_day = "2023-12-24"
    with pytest.raises(ValueError):
        cr.get_di(trade_date=non_business_day)
