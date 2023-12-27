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


def test_settlement_rate_with_old_holiday_list_1():
    # Reference date is before the new holiday calendar
    df = dip.get_di_data(reference_date="22-12-2023")
    result = df.query("contract_code == 'F33'")["settlement_rate"].values[0]
    assert result == 10.368


def test_settlement_rate_with_old_holiday_list_2():
    # Reference date is before the new holiday calendar
    df = dip.get_di_data(reference_date="22-12-2023")
    result = df.query("contract_code == 'N27'")["settlement_rate"].values[0]
    assert result == 9.809
