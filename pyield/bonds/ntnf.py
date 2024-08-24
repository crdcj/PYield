import numpy as np
import pandas as pd

from .. import bday
from .. import date_converter as dc
from .. import interpolator as it
from ..data import anbima
from . import bond_tools as bt

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


def rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the bond indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the maturities and the indicative rates.
    """
    return anbima.rates(reference_date, "NTN-F")[["MaturityDate", "IndicativeRate"]]


def maturities(reference_date: str | pd.Timestamp) -> pd.Series:
    """
    Fetch the NTN-F bond maturities available for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.Series: A Series of NTN-F bond maturities available for the reference date.
    """
    df_rates = rates(reference_date)
    return df_rates["MaturityDate"]


def _check_maturity_date(maturity: pd.Timestamp) -> None:
    """
    Check if the maturity date is a valid NTN-F maturity date.

    Args:
        maturity_date (pd.Timestamp): The maturity date to be checked.

    Raises:
        ValueError: If the maturity date is not the 1st of January.
    """
    if maturity.day != 1 or maturity.month not in COUPON_MONTHS:
        raise ValueError("NTN-F maturity date must be the 1st of January.")


def coupon_dates(
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
) -> pd.Series:
    """
    Generate all remaining coupon dates between a settlement date and a maturity date.
    The dates are exclusive for the settlement date and inclusive for the maturity date.
    Coupon payments are made on the 1st of January and July.
    The NTN-F bond is determined by its maturity date.

    Args:
        settlement (str | pd.Timestamp): The settlement date.
        maturity (str | pd.Timestamp): The maturity date.

    Returns:
        pd.Series: A Series containing the coupon dates between the settlement and
            maturity dates.
    """
    # Validate and normalize dates
    settlement = dc.convert_date(settlement)
    maturity = dc.convert_date(maturity)

    # Check if the maturity date is valid
    _check_maturity_date(maturity)

    # Check if maturity date is after the start date
    if maturity <= settlement:
        raise ValueError("Maturity date must be after the settlement date.")

    # Initialize loop variables
    coupon_date = maturity
    coupon_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while coupon_date > settlement:
        coupon_dates.append(coupon_date)
        # Move the coupon date back 6 months
        coupon_date -= pd.DateOffset(months=6)

    # Return the coupon dates as a sorted Series
    return pd.Series(coupon_dates).sort_values(ignore_index=True)


def cash_flows(
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
) -> pd.DataFrame:
    """
    Generate the cash flows for the NTN-F bond between the settlement (exclusive) and
    maturity dates (inclusive). The cash flows are the coupon payments and the final
    payment at maturity.

    Args:
        settlement (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.

    Returns:
        pd.DataFrame: A DataFrame containing the payment dates and the corresponding
            cash flows.
    """
    # Validate input dates
    settlement = dc.convert_date(settlement)
    maturity = dc.convert_date(maturity)
    _check_maturity_date(maturity)

    # Get the coupon payment dates between the settlement and maturity dates
    payment_dates = coupon_dates(settlement, maturity)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    cf_values = np.where(payment_dates == maturity, FINAL_PMT, COUPON_PMT)

    return pd.DataFrame(data={"PaymentDate": payment_dates, "CashFlow": cf_values})


def price(
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
    rate: float,
) -> float:
    """
    Calculate the NTN-F price using Anbima rules, which corresponds to the present
        value of the cash flows discounted at the given yield to maturity rate (YTM).

    Args:
        settlement (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        rate (float): The discount rate (yield to maturity) used to calculate the
            present value of the cash flows.

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
    df_cf = cash_flows(settlement, maturity)
    cf_values = df_cf["CashFlow"]
    bdays = bday.count(settlement, df_cf["PaymentDate"])
    byears = bt.truncate(bdays / 252, 14)
    discount_factors = (1 + rate) ** byears
    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    dcf = (cf_values / discount_factors).round(9)
    # Return the sum of the discounted cash flows truncated as per Anbima rules
    return bt.truncate(dcf.sum(), 6)


def _calculate_coupons_pv(
    bootstrap_df: pd.DataFrame,
    settlement: pd.Timestamp,
    maturity: pd.Timestamp,
) -> float:
    # Create a subset DataFrame with only the coupon payments (without last payment)
    cp_dates_wo_last = coupon_dates(settlement, maturity)[:-1]  # noqa
    df_coupons = bootstrap_df.query("MaturityDate in @cp_dates_wo_last").copy()
    df_coupons["Coupon"] = COUPON_PMT

    # Calculate the present value of the coupon payments
    pv = bt.calculate_present_value(
        cash_flows=df_coupons["Coupon"],
        rates=df_coupons["SpotRate"],
        periods=df_coupons["BDays"] / 252,
    )
    return pv


def spot_rates(
    settlement: str | pd.Timestamp,
    ltn_rates: pd.Series,
    ltn_maturities: pd.Series,
    ntnf_rates: pd.Series,
    ntnf_maturities: pd.Series,
) -> pd.DataFrame:
    """
    Calculate the spot rates for NTN-F bonds using the bootstrap method.

    The bootstrap method is a process used to determine spot rates from
    the yields of a series of bonds. It involves iteratively solving for
    the spot rates that discount each bond's cash flows to its current
    price.

    Args:
        settlement (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        ltn_rates (pd.Series): The LTN known rates.
        ltn_maturities (pd.Series): The LTN known maturities.
        ntnf_rates (pd.Series): The NTN-F known rates.
        ntnf_maturities (pd.Series): The NTN-F known maturities.

    Returns:
        pd.DataFrame: A DataFrame containing the maturity dates and
            the corresponding spot rates.
    """
    # Process and validate the input data
    settlement = dc.convert_date(settlement)

    # Create flat forward interpolators for LTN and NTN-F rates
    ltn_rate_interpolator = it.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, ltn_maturities),
        known_rates=ltn_rates,
    )
    ntnf_rate_interpolator = it.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, ntnf_maturities),
        known_rates=ntnf_rates,
    )

    # Determine the last maturity dates for LTN and NTN-F rates
    last_ltn = ltn_maturities.max()
    last_ntnf = ntnf_maturities.max()

    # Generate all coupon dates up to the last NTN-F maturity date
    all_coupon_dates = coupon_dates(settlement, last_ntnf)

    # Create a DataFrame with all coupon dates and the corresponding YTM
    df_spot = pd.DataFrame(data=all_coupon_dates, columns=["MaturityDate"])
    df_spot["BDays"] = bday.count(start=settlement, end=df_spot["MaturityDate"])
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
        coupons_pv = _calculate_coupons_pv(df_spot, settlement, maturity)
        ytm = df_spot.at[index, "YTM"]
        bond_price = price(settlement, maturity, ytm)
        spot_rate = (FINAL_PMT / (bond_price - coupons_pv)) ** (252 / bdays) - 1
        df_spot.at[index, "SpotRate"] = spot_rate

    return df_spot


def di_spreads(reference_date: str | pd.Timestamp) -> pd.DataFrame:
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
    # Fetch DI Spreads for the reference date
    df = bt.di_spreads(reference_date)
    df.query("BondType == 'NTN-F'", inplace=True)
    df.sort_values(["MaturityDate"], ignore_index=True, inplace=True)
    return df[["MaturityDate", "DISpread"]]


def di_net_spread(  # noqa
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
    ytm: float,
    di_rates: pd.Series,
    di_expirations: pd.Series,
    initial_guess: float | None = None,
) -> float:
    """
    Calculate the net DI spread for a bond given the YTM and the DI rates.

    This function determines the spread over the DI curve that equates the present value
    of the bond's cash flows to its market price. It interpolates the DI rates to match
    the bond's cash flow payment dates and uses the Brent method to find the spread
    (in bps) that zeroes the difference between the bond's market price and its
    discounted cash flows.

    Args:
        settlement (str | pd.Timestamp): The bond settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (str | pd.Timestamp): The bond maturity date in 'DD-MM-YYYY'
            format or a pandas Timestamp.
        ytm (float): The yield to maturity (YTM) of the bond.
        di_rates (pd.Series): A Series of DI rates.
        di_expirations (pd.Series): A list or Series of DI expiration dates.
        initial_guess (float, optional): An initial guess for the spread. Defaults to
            None.

    Returns:
        float: The net DI spread in basis points.
    """
    # Create an interpolator for the DI rates using the flat-forward method
    settlement = dc.convert_date(settlement)
    maturity = dc.convert_date(maturity)

    ff_interpolator = it.Interpolator(
        "flat_forward",
        bday.count(settlement, di_expirations),
        di_rates,
    )

    # Ensure the DI data is valid
    if len(di_rates) != len(di_expirations):
        raise ValueError("di_rates and di_expirations must have the same length.")
    if len(di_rates) == 0:
        return float("nan")

    # Calculate cash flows and business days between settlement and payment dates
    df = cash_flows(settlement, maturity).reset_index()
    df["BDays"] = bday.count(settlement, df["PaymentDate"])

    # Calculate business years (252 business days per year)
    byears = bday.count(settlement, df["PaymentDate"]) / 252
    di_interp = df["BDays"].apply(ff_interpolator)
    bond_price = price(settlement, maturity, ytm)
    bond_cash_flows = df["CashFlow"]

    def price_difference(p):
        # Calculate the difference between the bond's price and its disc. cash flows
        return (bond_cash_flows / (1 + di_interp + p) ** byears).sum() - bond_price

    # Bisection method to find the root
    def bisection_method(func, a, b, tol=1e-8, maxiter=100):
        fa, fb = func(a), func(b)
        if fa * fb > 0:
            raise ValueError("Function does not change sign in the interval.")

        for _ in range(maxiter):
            midpoint = (a + b) / 2
            fmid = func(midpoint)
            if np.abs(fmid) < tol or (b - a) / 2 < tol:
                return midpoint
            if fmid * fa < 0:
                b, fb = midpoint, fmid
            else:
                a, fa = midpoint, fmid

        return (a + b) / 2

    try:
        if initial_guess is not None:
            a = initial_guess - 50 / 10_000  # 50 bps below the initial guess
            b = initial_guess + 50 / 10_000  # 50 bps above the initial guess
        else:
            a = -0.01  # Initial guess of -100 bps
            b = 0.01  # Initial guess of 100 bps

        # Find the spread (p) that zeroes the price difference
        p_solution = bisection_method(price_difference, a, b)
        # Convert the solution to basis points (bps) and round to two decimal places
        p_solution = round((p_solution * 10_000), 2)
    except ValueError:
        # If no solution is found, return NaN
        p_solution = float("nan")

    return p_solution
