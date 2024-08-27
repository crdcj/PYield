from collections.abc import Callable

import numpy as np
import pandas as pd

from .. import bday
from .. import date_converter as dc
from .. import interpolator as it
from ..data import anbima, di
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
    ntnf_rates = anbima.rates(reference_date, "NTN-F")
    if ntnf_rates.empty:
        return pd.DataFrame()
    return ntnf_rates[["MaturityDate", "IndicativeRate"]]


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
    adj_payment_dates: bool = False,
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
        adj_payment_dates (bool): If True, adjust the payment dates to the next
            business day.

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

    df = pd.DataFrame(data={"PaymentDate": payment_dates, "CashFlow": cf_values})

    if adj_payment_dates:
        df["PaymentDate"] = bday.offset(df["PaymentDate"], 0)

    return df


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
    cf_df = cash_flows(settlement, maturity)
    cf_values = cf_df["CashFlow"]
    bdays = bday.count(settlement, cf_df["PaymentDate"])
    byears = bt.truncate(bdays / 252, 14)
    discount_factors = (1 + rate) ** byears
    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    dcf = (cf_values / discount_factors).round(9)
    # Return the sum of the discounted cash flows truncated as per Anbima rules
    return bt.truncate(dcf.sum(), 6)


def spot_rates(
    settlement: str | pd.Timestamp,
    ltn_maturities: pd.Series,
    ltn_rates: pd.Series,
    ntnf_maturities: pd.Series,
    ntnf_rates: pd.Series,
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

    # Generate all coupon dates up to the last NTN-F maturity date
    all_coupon_dates = coupon_dates(settlement, ntnf_maturities.max())

    # Create a DataFrame with all coupon dates and the corresponding YTM
    df = pd.DataFrame(data=all_coupon_dates, columns=["MaturityDate"])
    df["BDays"] = bday.count(start=settlement, end=df["MaturityDate"])
    df["BYears"] = df["BDays"] / 252
    df["Coupon"] = COUPON_PMT
    df["YTM"] = df["BDays"].apply(ntnf_rate_interpolator)

    # The Bootstrap loop to calculate spot rates
    for index, row in df.iterrows():
        if row["MaturityDate"] <= ltn_maturities.max():
            # Use LTN rates for maturities before the last LTN maturity date
            df.at[index, "SpotRate"] = ltn_rate_interpolator(row["BDays"])
            continue

        # Calculate the present value of the coupon payments
        cf_dates = coupon_dates(settlement, row["MaturityDate"])[:-1]  # noqa
        cf_df = df.query("MaturityDate in @cf_dates").reset_index(drop=True)
        cf_present_value = bt.calculate_present_value(
            cash_flows=cf_df["Coupon"],
            rates=cf_df["SpotRate"],
            periods=cf_df["BDays"] / 252,
        )

        bond_price = price(settlement, row["MaturityDate"], row["YTM"])
        price_factor = FINAL_PMT / (bond_price - cf_present_value)
        df.at[index, "SpotRate"] = price_factor ** (1 / row["BYears"]) - 1

    # Remove temporary columns and return the spot rates DataFrame
    return df[["MaturityDate", "BDays", "YTM", "SpotRate"]].copy()


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


# Bisection method to find the root
def _bisection_method(func, a, b, tol=1e-8, maxiter=100):
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


def _solve_spread(
    price_difference_func: Callable,
    initial_guess: float | None = None,
) -> float:
    """
    Solve for the spread that zeroes the price difference using a bisection method.

    Args:
        price_difference_func (callable): The function that computes the difference
            between the bond's market price and its discounted cash flows.
        initial_guess (float, optional): An initial guess for the spread.

    Returns:
        float: The solution for the spread in bps or NaN if no solution is found.
    """
    try:
        if initial_guess is not None:
            # range_width_bps below the initial guess
            a = initial_guess - 0.005  # 50 bps
            # range_width_bps above the initial guess
            b = initial_guess + 0.005  # 50 bps
        else:
            a = -0.01  # Initial guess of -100 bps
            b = 0.01  # Initial guess of 100 bps

        # Find the spread (p) that zeroes the price difference
        p_solution = _bisection_method(price_difference_func, a, b)
    except ValueError:
        # If no solution is found, return NaN
        p_solution = float("NaN")

    return p_solution


def di_net_spread(  # noqa
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
    ytm: float,
    di_expirations: pd.Series,
    di_rates: pd.Series,
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
            None. A good initial guess is the DI gross spread for the bond.

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
        return float("NaN")

    # Calculate cash flows and business days between settlement and payment dates
    df = cash_flows(settlement, maturity).reset_index()
    df["BDays"] = bday.count(settlement, df["PaymentDate"])

    byears = bday.count(settlement, df["PaymentDate"]) / 252
    di_interp = df["BDays"].apply(ff_interpolator)
    bond_price = price(settlement, maturity, ytm)
    bond_cash_flows = df["CashFlow"]

    def price_difference(p):
        # Difference between the bond's price and its discounted cash flows
        return (bond_cash_flows / (1 + di_interp + p) ** byears).sum() - bond_price

    # Solve for the spread that zeroes the price difference using the bisection method
    p_solution = _solve_spread(price_difference, initial_guess)
    # Convert the solution to basis points (bps) and round to two decimal places
    return round((p_solution * 10_000), 2)


def premium(
    settlement: str | pd.Timestamp,
    ntnf_maturity: str | pd.Timestamp,
    ntnf_rate: float,
    di_expirations: pd.Series,
    di_rates: pd.Series,
) -> float:
    ntnf_maturity = dc.convert_date(ntnf_maturity)
    settlement = dc.convert_date(settlement)

    df = cash_flows(settlement, ntnf_maturity, adj_payment_dates=True)
    df["BDays"] = bday.count(settlement, df["PaymentDate"])
    df["BYears"] = df["BDays"] / 252

    ff_interpolator = it.Interpolator(
        "flat_forward",
        bday.count(settlement, di_expirations),
        di_rates,
    )

    df["DIRate"] = df["BDays"].apply(ff_interpolator)

    # Calculate the present value of the cash flows using the DI rate
    bond_price = bt.calculate_present_value(
        cash_flows=df["CashFlow"],
        rates=df["DIRate"],
        periods=df["BDays"] / 252,
    )

    # Calculate the rate corresponding to this price
    def price_difference(ytm):
        # The ytm that zeroes the price difference
        return (df["CashFlow"] / (1 + ytm) ** df["BYears"]).sum() - bond_price

    # Solve for the YTM that zeroes the price difference
    di_ytm = _solve_spread(price_difference, ntnf_rate)

    factor_ntnf = (1 + ntnf_rate) ** (1 / 252)
    factor_di = (1 + di_ytm) ** (1 / 252)
    premium_np = (factor_ntnf - 1) / (factor_di - 1)
    return round(float(premium_np), 6)


def historical_premium(
    reference_date: str | pd.Timestamp,
    maturity: str | pd.Timestamp,  # noqa
) -> float:
    reference_date = dc.convert_date(reference_date)
    maturity = dc.convert_date(maturity)

    df_ntnf = rates(reference_date)
    if df_ntnf.empty:
        return float("NaN")

    ntnf_ytms = df_ntnf.query("MaturityDate == @maturity")["IndicativeRate"]
    if ntnf_ytms.empty:
        return float("NaN")
    ntnf_ytm = float(ntnf_ytms.iloc[0])

    df = cash_flows(reference_date, maturity, adj_payment_dates=True)
    df["BDays"] = bday.count(reference_date, df["PaymentDate"])
    df["BYears"] = df["BDays"] / 252
    df["DIRate"] = df["PaymentDate"].apply(lambda x: di.rate(reference_date, x))

    # Calculate the present value of the cash flows using the DI rate
    bond_price = bt.calculate_present_value(
        cash_flows=df["CashFlow"],
        rates=df["DIRate"],
        periods=df["BDays"] / 252,
    )

    # Calculate the rate corresponding to this price
    def price_difference(ytm):
        # The ytm that zeroes the price difference
        return (df["CashFlow"] / (1 + ytm) ** df["BYears"]).sum() - bond_price

    # Solve for the YTM that zeroes the price difference
    di_ytm = _solve_spread(price_difference, ntnf_ytm)

    factor_ntnf = (1 + ntnf_ytm) ** (1 / 252)
    factor_di = (1 + di_ytm) ** (1 / 252)

    return float((factor_ntnf - 1) / (factor_di - 1))
