import numpy as np
import pandas as pd

import pyield.date_converter as dc
import pyield.tn.tools as bt
from pyield import anbima, bday
from pyield.date_converter import DateScalar

"""
Constants calculated as per Anbima Rules and in base 100
Valid for NTN-C bonds with maturity date 01-01-2031:
PRINCIPAL = 100
COUPON_RATE = (0.12 + 1) ** 0.5 - 1  # 12% annual rate compounded semi-annually
COUPON_PMT = round(100 * COUPON_RATE, 6) -> 5.830052
FINAL_PMT = principal + last coupon payment = 100 + COUPON_PMT = 105.830052

All other NTN-C bonds will have different values for COUPON_PMT and FINAL_PMT:
COUPON_RATE = (0.06 + 1) ** 0.5 - 1  # 6% annual rate compounded semi-annually
COUPON_PMT = round(100 * COUPON_RATE, 6) -> 2.956301
FINAL_PMT = principal + last coupon payment = 100 + 2.956301

"""
COUPON_DAY = 1
COUPON_PMT_2031 = 5.830052
FINAL_PMT_2031 = 105.830052

COUPON_PMT = 2.956301
FINAL_PMT = 102.956301


def _get_coupon_pmt(maturity: pd.Timestamp) -> float:
    if maturity.year == 2031:  # noqa
        return COUPON_PMT_2031
    return COUPON_PMT


def _get_final_pmt(maturity: pd.Timestamp) -> float:
    if maturity.year == 2031:  # noqa
        return FINAL_PMT_2031
    return FINAL_PMT


def data(date: DateScalar) -> pd.DataFrame:
    """
    Fetch the LTN Anbima indicative rates for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame with columns "MaturityDate" and "IndicativeRate".

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.data("21-03-2025")
          ReferenceDate BondType MaturityDate  IndicativeRate        Price
        0    2025-03-21    NTN-C   2031-01-01        0.067626  8347.348705
    """
    return anbima.tpf_data(date, "NTN-C")


def payment_dates(
    settlement: DateScalar,
    maturity: DateScalar,
) -> pd.Series:
    """
    Generate all remaining coupon dates between a given date and the maturity date.
    The dates are inclusive. The NTN-C bond is determined by its maturity date.

    Args:
        settlement (DateScalar): The settlement date (exlusive) to start generating
            the coupon dates.
        maturity (DateScalar): The maturity date.

    Returns:
        pd.Series: Series of coupon dates within the specified range.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.payment_dates("21-03-2025", "01-01-2031")
        0    2025-07-01
        1    2026-01-01
        2    2026-07-01
        3    2027-01-01
        4    2027-07-01
        5    2028-01-01
        6    2028-07-01
        7    2029-01-01
        8    2029-07-01
        9    2030-01-01
        10   2030-07-01
        11   2031-01-01
        dtype: datetime64[ns]
    """
    # Validate and normalize dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

    # Check if maturity date is after the start date
    if maturity < settlement:
        raise ValueError("Maturity date must be after the start date.")

    # Initialize loop variables
    coupon_dates = maturity
    cp_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while coupon_dates > settlement:
        cp_dates.append(coupon_dates)
        # Move the coupon date back 6 months
        coupon_dates -= pd.DateOffset(months=6)

    return pd.Series(cp_dates).sort_values(ignore_index=True)


def cash_flows(
    settlement: DateScalar,
    maturity: DateScalar,
) -> pd.DataFrame:
    """
    Generate the cash flows for NTN-C bonds between the settlement and maturity dates.

    Args:
        settlement (DateScalar): The settlement date (exclusive) to start generating
            the cash flows.
        maturity (DateScalar): The maturity date of the bond.

    Returns:
        pd.DataFrame: DataFrame with columns "PaymentDate" and "CashFlow".

    Returned columns:
        - PaymentDate: The payment date of the cash flow
        - CashFlow: Cash flow value for the bond

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.cash_flows("21-03-2025", "01-01-2031")
           PaymentDate    CashFlow
        0   2025-07-01    5.830052
        1   2026-01-01    5.830052
        2   2026-07-01    5.830052
        3   2027-01-01    5.830052
        4   2027-07-01    5.830052
        5   2028-01-01    5.830052
        6   2028-07-01    5.830052
        7   2029-01-01    5.830052
        8   2029-07-01    5.830052
        9   2030-01-01    5.830052
        10  2030-07-01    5.830052
        11  2031-01-01  105.830052
    """
    # Validate and normalize dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

    # Get the coupon dates between the settlement and maturity dates
    p_dates = payment_dates(settlement, maturity)

    # Get the right coupon payment and final payment values
    coupon_pmt = _get_coupon_pmt(maturity)
    final_pmt = _get_final_pmt(maturity)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    cfs = np.where(p_dates == maturity, final_pmt, coupon_pmt).tolist()

    # Return a dataframe with the payment dates and cash flows
    return pd.DataFrame(data={"PaymentDate": p_dates, "CashFlow": cfs})


def quotation(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the NTN-C quotation in base 100 using Anbima rules.

    Args:
        settlement (DateScalar): The settlement date of the operation.
        maturity (DateScalar): The maturity date of the NTN-C bond.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-C.

    Returns:
        float: The NTN-C quotation truncated to 4 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - The semi-annual coupon is set to 2.956301, which represents a 6% annual
          coupon rate compounded semi-annually and rounded to 6 decimal places as per
          Anbima rules.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.quotation("21-03-2025", "01-01-2031", 0.067626)
        126.4958
    """
    # Validate and normalize dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

    cf_df = cash_flows(settlement, maturity)
    cf_dates = cf_df["PaymentDate"]
    cf_values = cf_df["CashFlow"]

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement, cf_dates)

    # Calculate the number of periods truncated as per Anbima rules
    num_of_years = bt.truncate(bdays / 252, 14)

    discount_factor = (1 + rate) ** num_of_years

    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    cf_present_value = (cf_values / discount_factor).round(10)

    # Return the quotation (the dcf sum) truncated as per Anbima rules
    return bt.truncate(cf_present_value.sum(), 4)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calculate the NTN-C price using Anbima rules.

    price = VNA * quotation / 100

    Args:
        vna (float): The nominal value of the NTN-C bond.
        quotation (float): The NTN-C quotation in base 100.

    Returns:
        float: The NTN-C price truncated to 6 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.price(6598.913723, 126.4958)
        8347.348705
    """
    return bt.truncate(vna * quotation / 100, 6)


def duration(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the Macaulay duration of the NTN-C bond in business years.

    Args:
        settlement (DateScalar): The settlement date of the operation.
        maturity (DateScalar): The maturity date of the NTN-C bond.
        rate (float): The discount rate used to calculate the duration.

    Returns:
        float: The Macaulay duration of the NTN-C bond in business years.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.duration("21-03-2025", "01-01-2031", 0.067626)
        4.405363320448003
    """
    # Return NaN if any input is NaN
    if any(pd.isna(x) for x in [settlement, maturity, rate]):
        return float("NaN")

    # Validate and normalize dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

    df = cash_flows(settlement, maturity)
    df["BY"] = bday.count(settlement, df["PaymentDate"]) / 252
    df["DCF"] = df["CashFlow"] / (1 + rate) ** df["BY"]
    duration = (df["DCF"] * df["BY"]).sum() / df["DCF"].sum()
    # Return the duration as native float
    return float(duration)
