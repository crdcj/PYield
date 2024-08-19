import numpy as np
import pandas as pd
from scipy.optimize import brentq

from .. import bday, di
from .. import date_converter as dc
from ..fetchers import anbima as an
from ..interpolator import Interpolator
from . import utils as ut

"""
Constants calculated as per Anbima Rules
COUPON_RATE = (0.10 + 1) ** 0.5 - 1  -> 10% annual rate compounded semi-annually
FACE_VALUE = 1000
COUPON_PMT = round(FACE_VALUE * COUPON_RATE, 5)
FINAL_PMT = FACE_VALUE + COUPON_PMT
"""
COUPON_DAY = 1
COUPON_MONTHS = {1, 7}
COUPON_PMT = 48.80885
FINAL_PMT = 1048.80885

di_data = di.DIData()


def check_maturity_date(maturity_date: pd.Timestamp) -> None:
    """
    Check if the maturity date is a valid NTN-F maturity date.

    Args:
        maturity_date (pd.Timestamp): The maturity date to be checked.

    Raises:
        ValueError: If the maturity date is not the 1st of January.
    """
    if maturity_date.day != 1 or maturity_date.month not in COUPON_MONTHS:
        raise ValueError("NTN-F maturity date must be the 1st of January.")


def coupon_dates(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
) -> pd.Series:
    """
    Generate all remaining coupon dates between a settlement date and a maturity date.
    The dates are exclusive for the settlement date and inclusive for the maturity date.
    Coupon payments are made on the 1st of January and July.
    The NTN-F bond is determined by its maturity date.

    Args:
        settlement_date (str | pd.Timestamp): The settlement date.
        maturity_date (str | pd.Timestamp): The maturity date.

    Returns:
        pd.Series: Series of coupon dates within the specified range.
    """
    # Validate and normalize dates
    settlement_date = dc.convert_date(settlement_date)
    maturity_date = dc.convert_date(maturity_date)

    # Check if the maturity date is valid
    check_maturity_date(maturity_date)

    # Check if maturity date is after the start date
    if maturity_date <= settlement_date:
        raise ValueError("Maturity date must be after the start date.")

    # Initialize loop variables
    coupon_date = maturity_date
    coupon_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while coupon_date > settlement_date:
        coupon_dates.append(coupon_date)
        # Move the coupon date back 6 months
        coupon_date -= pd.DateOffset(months=6)

    # Return the coupon dates as a sorted Series
    return pd.Series(coupon_dates).sort_values(ignore_index=True)


def cash_flows(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
) -> pd.Series:
    """
    Generate the cash flows for the NTN-F bond between the settlement (exclusive) and
    maturity dates (inclusive). The cash flows are the coupon payments and the final
    payment at maturity.

    Args:
        settlement_date (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity_date (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.

    Returns:
        pd.Series: Series of cash flows within the specified range.
    """
    # Validate input dates
    settlement_date = dc.convert_date(settlement_date)
    maturity_date = dc.convert_date(maturity_date)
    check_maturity_date(maturity_date)

    # Get the coupon payment dates between the settlement and maturity dates
    payment_dates = coupon_dates(settlement_date, maturity_date)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    cfs = np.where(payment_dates == maturity_date, FINAL_PMT, COUPON_PMT)

    df = pd.DataFrame(data=cfs, index=payment_dates, columns=["CashFlow"])
    df.index.name = "PaymentDate"

    return df["CashFlow"]


def price(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
    ytm_rate: float,
) -> float:
    """
    Calculate the NTN-F price using Anbima rules, which corresponds to the present
        value of the cash flows discounted at the given yield to maturity rate (YTM).

    Args:
        settlement_date (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity_date (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        ytm_rate (float): The discount rate used to calculate the present value of
            the cash flows.

    Returns:
        float: The NTN-F price using Anbima rules.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - The semi-annual coupon is set to 48.81, which represents a 10% annual
          coupon rate compounded semi-annually and rounded to 5 decimal places as per
          Anbima rules.

    Examples:
        >>> price("05-07-2024", "01-01-2035", 0.11921)
        895.359254
    """
    df_cf = cash_flows(settlement_date, maturity_date).reset_index()
    cfs = df_cf["CashFlow"]
    bdays = bday.count(settlement_date, df_cf["PaymentDate"])
    byears = ut.truncate(bdays / 252, 14)
    discount_factors = (1 + ytm_rate) ** byears
    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    dcf = (cfs / discount_factors).round(9)
    # Return the sum of the discounted cash flows truncated as per Anbima rules
    return ut.truncate(dcf.sum(), 6)


def anbima_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-F Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return an.anbima_data(reference_date, "NTN-F")


def indicative_rates(
    reference_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp | None = None,
) -> pd.Series | float:
    """
    Fetch NTN-F Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.Series: A Series containing the rates indexed by maturity date.
    """
    rates = an.get_anbima_rates(reference_date, "NTN-F")

    if maturity_date:
        maturity_date = dc.convert_date(maturity_date)
        if maturity_date in rates.index:
            return float(rates[maturity_date])
        else:
            return float("nan")
    return rates


def anbima_historical_rates(maturity_date: str | pd.Timestamp) -> pd.Series:
    """
    Fetch historical NTN-F Anbima indicative rates for the given maturity date.

    Args:
        maturity_date (str | pd.Timestamp): The maturity date of the bond.

    Returns:
        pd.Series: A Series containing the rates for the given maturity date.
    """
    return an.get_anbima_historical_rates("NTN-F", maturity_date)


def _calculate_coupons_pv(
    bootstrap_df: pd.DataFrame,
    settlement_date: pd.Timestamp,
    maturity_date: pd.Timestamp,
) -> float:
    # Create a subset DataFrame with only the coupon payments (without last payment)
    cp_dates_wo_last = coupon_dates(settlement_date, maturity_date)[:-1]  # noqa
    df_coupons = bootstrap_df.query("MaturityDate in @cp_dates_wo_last").copy()
    df_coupons["Coupon"] = COUPON_PMT

    # Calculate the present value of the coupon payments
    pv = ut.calculate_present_value(
        cash_flows=df_coupons["Coupon"],
        discount_rates=df_coupons["SpotRate"],
        time_periods=df_coupons["BDays"] / 252,
    )
    return pv


def spot_rates(
    settlement_date: str | pd.Timestamp,
    ltn_rates: pd.Series,
    ntnf_rates: pd.Series,
) -> pd.DataFrame:
    """
    Calculate the spot rates for NTN-F bonds using the bootstrap method.

    The bootstrap method is a process used to determine spot rates from
    the yields of a series of bonds. It involves iteratively solving for
    the spot rates that discount each bond's cash flows to its current
    price.

    Args:
        settlement_date (str | pd.Timestamp): The settlement date in as
            a pandas Timestamp or a string in 'DD-MM-YYYY' format.
        ltn_rates (pd.Series): The LTN known rates, indexed by maturity date.
        ntnf_rates (pd.Series): The NTN-F known rates, indexed by maturity
            date.

    Returns:
        pd.DataFrame: A DataFrame containing the maturity dates and
            the corresponding spot rates.
    """
    # Process and validate the input data
    settlement_date = dc.convert_date(settlement_date)
    ltn_rates = ut.standardize_rates(ltn_rates)
    ntnf_rates = ut.standardize_rates(ntnf_rates)

    # Create flat forward interpolators for LTN and NTN-F rates
    ltn_rate_interpolator = Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement_date, ltn_rates.index),
        known_rates=ltn_rates,
    )
    ntnf_rate_interpolator = Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement_date, ntnf_rates.index),
        known_rates=ntnf_rates,
    )

    # Determine the last maturity dates for LTN and NTN-F rates
    last_ltn = ltn_rates.index.max()
    last_ntnf = ntnf_rates.index.max()

    # Generate all coupon dates up to the last NTN-F maturity date
    all_coupon_dates = coupon_dates(settlement_date, last_ntnf)

    # Create a DataFrame with all coupon dates and the corresponding YTM
    df_spot = pd.DataFrame(data=all_coupon_dates, columns=["MaturityDate"])
    df_spot["BDays"] = bday.count(start=settlement_date, end=df_spot["MaturityDate"])
    df_spot["YTM"] = df_spot["BDays"].apply(ntnf_rate_interpolator)

    # The Bootstrap loop to calculate spot rates
    for index in df_spot.index:
        maturity = df_spot.at[index, "MaturityDate"]
        bdays = df_spot.at[index, "BDays"]

        if maturity <= last_ltn:
            # Use LTN rates for maturities before the last LTN maturity date
            df_spot.at[index, "SpotRate"] = ltn_rate_interpolator(bdays)
            continue

        # Calculate the spot rate for the bond
        coupons_pv = _calculate_coupons_pv(df_spot, settlement_date, maturity)
        ytm = df_spot.at[index, "YTM"]
        bond_price = price(settlement_date, maturity, ytm)
        spot_rate = (FINAL_PMT / (bond_price - coupons_pv)) ** (252 / bdays) - 1
        df_spot.at[index, "SpotRate"] = spot_rate

    return df_spot


def gross_di_spreads(reference_date: str | pd.Timestamp) -> pd.Series:
    """
    Calculates the DI spread for the NTN-F based on ANBIMA's indicative rates.

    This function fetches the indicative rates for the NTN-F bonds and the DI futures
    rates and calculates the spread between these rates in basis points.

    Parameters:
        reference_date (str | pd.Timestamp, optional): The reference date for the
            spread calculation.

    Returns:
        pd.Series: A pandas series containing the calculated spreads in basis points
            indexed by maturity dates.
    """
    reference_date = dc.convert_date(reference_date)
    # Fetch DI Spreads for the reference date
    df = ut.di_spreads(reference_date)
    df.query("BondType == 'NTN-F'", inplace=True)
    df.sort_values(["MaturityDate"], ignore_index=True, inplace=True)
    df.set_index("MaturityDate", inplace=True)
    return df["DISpread"]


def net_di_spread(
    reference_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
) -> float:
    reference_date = dc.convert_date(reference_date)
    maturity_date = dc.convert_date(maturity_date)
    settlement_date = bday.offset(reference_date, 0)

    # Fetch DI rates for the reference date
    df_di = di_data.settlement_rates(reference_date, adjust_exp_date=True)

    ff_interpolator = Interpolator(
        "flat_forward",
        bday.count(settlement_date, df_di["ExpirationDate"]),
        df_di["SettlementRate"],
    )

    # Get the corresponding YTM for the NTN-F bond
    ytm = indicative_rates(reference_date, maturity_date)

    df = cash_flows(settlement_date, maturity_date).reset_index()
    df["BDays"] = bday.count(settlement_date, df["PaymentDate"])

    byears = bday.count(settlement_date, df["PaymentDate"]) / 252
    di_rates = df["BDays"].apply(ff_interpolator)
    bond_price = price(settlement_date, maturity_date, ytm)
    bond_cash_flows = df["CashFlow"]

    def price_difference(p):
        return (bond_cash_flows / (1 + di_rates + p) ** (byears)).sum() - bond_price

    try:
        # Tentar encontrar o valor de p que zera a diferença entre os preços
        # Intervalo de busca de -0.01 a 0.01 (100 bps)
        p_solution = brentq(price_difference, -0.01, 0.01, maxiter=100)
    except (ValueError, RuntimeError):
        # Se não houver solução, retornar float('nan')
        p_solution = float("nan")

    return p_solution * 10_000
