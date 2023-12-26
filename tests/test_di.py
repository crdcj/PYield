import pandas as pd

from pyield import di_processing as dp


def test_convert_old_contract_code():
    contact_code = "JAN3"
    reference_date = pd.Timestamp("2001-05-21")
    maturity_date = pd.Timestamp("2003-01-01")
    assert dp.convert_old_contract_code(contact_code, reference_date) == maturity_date
