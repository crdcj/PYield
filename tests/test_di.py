import pandas as pd

from pyield import di_processing as dip


def test_convert_old_contract_code():
    contact_code = "JAN3"  # Valid contract code
    reference_date = pd.Timestamp("2001-05-21")
    result = dip.convert_old_contract_code(contact_code, reference_date)
    contract_maturity = pd.Timestamp("2003-01-01")
    assert result == contract_maturity


def test_convert_invalid_old_contract_code():
    contact_code = "J3"  # Invalid contract code
    reference_date = pd.Timestamp("2001-01-01")
    # Must return NaT
    result = dip.convert_old_contract_code(contact_code, reference_date)
    assert pd.isnull(result)


def test_convert_new_contract_code():
    contact_code = "F23"  # Valid contract code
    result = dip.convert_contract_code(contact_code)
    contract_maturity = pd.Timestamp("2023-01-01")
    assert result == contract_maturity


def test_settlement_rate_with_old_holiday_list():
    settlement_rates = {
        "N27": 9.809,
        "F33": 10.368,
    }

    # 22-12-2023 is before the new holiday calendar
    df = dip.get_di_data(reference_date="22-12-2023")
    contract_codes = list(settlement_rates.keys())
    result = df.query("contract_code in @contract_codes")["settlement_rate"].to_list()
    assert result == list(settlement_rates.values())


def test_settlement_rates_with_current_holiday_list():
    settlement_rates = {
        "F24": 11.644,
        "J24": 11.300,
        "N24": 10.786,
        "V24": 10.321,
        "F25": 10.031,
        "J25": 9.852,
        "N25": 9.715,
        "V25": 9.651,
        "F26": 9.583,
        "N26": 9.631,
        "F27": 9.683,
        "N27": 9.794,
        "F29": 10.042,
        "F31": 10.240,
        "F33": 10.331,
    }

    df = dip.get_di_data(reference_date="26-12-2023")
    contract_codes = list(settlement_rates.keys())
    results = df.query("contract_code in @contract_codes")["settlement_rate"].to_list()
    assert results == list(settlement_rates.values())
