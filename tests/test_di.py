import pandas as pd

from pyield import di_futures as dif
from pyield import di_url as diu


def test_valid_old_contract_code1():
    contact_code = "JAN3"  # Valid contract code
    reference_date = pd.Timestamp("2001-05-21")
    result = diu.get_old_expiration_date(contact_code, reference_date)
    contract_expiration = pd.Timestamp("2003-01-02")
    assert result == contract_expiration


def test_valid_old_contract_code2():
    contact_code = "JAN3"  # Valid contract code
    reference_date = pd.Timestamp("1990-01-01")
    result = diu.get_old_expiration_date(contact_code, reference_date)
    contract_expiration = pd.Timestamp("1993-01-04")
    assert result == contract_expiration


def test_invalid_old_contract_code():
    contact_code = "J3"  # Invalid contract code
    reference_date = pd.Timestamp("2001-01-02")
    # Must return NaT
    result = diu.get_old_expiration_date(contact_code, reference_date)
    assert pd.isnull(result)


def test_new_contract_code():
    contact_code = "F23"  # Valid contract code
    result = dif.get_expiration_date(contact_code)
    contract_expiration = pd.Timestamp("2023-01-02")
    assert result == contract_expiration


def test_settlement_rate_with_old_holiday_list():
    settlement_rates = {
        "N27": 0.09809,
        "F33": 0.10368,
    }

    # 22-12-2023 is before the new holiday calendar
    df = dif.get_di(reference_date="2023-12-22")
    contract_codes = list(settlement_rates.keys())  # noqa: F841
    result = df.query("contract_code in @contract_codes")["settlement_rate"].to_list()
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

    df = dif.get_di(reference_date="2023-12-26")
    contract_codes = list(settlement_rates.keys())  # noqa: F841
    results = df.query("contract_code in @contract_codes")["settlement_rate"].to_list()
    assert results == list(settlement_rates.values())


def test_invalid_date():
    result = dif.get_di(reference_date="2023-12-24")
    assert result.empty
