from collections.abc import Callable

import numpy as np
import pandas as pd

from pyield import bday
from pyield import date_converter as dc
from pyield import interpolator as ip
from pyield.anbima import tpf
from pyield.b3 import di
from pyield.date_converter import DateScalar
from pyield.tpf import tools as tt

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


def rates(date: DateScalar) -> pd.DataFrame:
    """
    Fetch the bond indicative rates for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame with columns "MaturityDate" and "IndicativeRate".

    Examples:
        >>> yd.ntnf.rates("23-08-2024")
          MaturityDate  IndicativeRate
        0   2025-01-01        0.107692
        1   2027-01-01        0.115109
        2   2029-01-01        0.116337
        3   2031-01-01        0.117008
        4   2033-01-01        0.116307
        5   2035-01-01        0.116586
    """
    ntnf_rates = tpf.tpf_rates(date, "NTN-F")
    if ntnf_rates.empty:
        return pd.DataFrame()
    return ntnf_rates[["MaturityDate", "IndicativeRate"]]


def maturities(date: DateScalar) -> pd.Series:
    """
    Fetch the NTN-F bond maturities available for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.Series: A Series of NTN-F bond maturities available for the reference date.

    Examples:
        >>> yd.ntnf.maturities("23-08-2024")
        0   2025-01-01
        1   2027-01-01
        2   2029-01-01
        3   2031-01-01
        4   2033-01-01
        5   2035-01-01
        dtype: datetime64[ns]

    """
    df_rates = rates(date)
    s_maturities = df_rates["MaturityDate"]
    s_maturities.name = None
    return s_maturities


def _check_maturity_date(maturity: pd.Timestamp) -> None:
    """
    Check if the maturity date is a valid NTN-F maturity date.

    Args:
        maturity (pd.Timestamp): The maturity date to be checked.

    Raises:
        ValueError: If the maturity date is not the 1st of January.
    """
    if maturity.day != 1 or maturity.month not in COUPON_MONTHS:
        raise ValueError("NTN-F maturity date must be the 1st of January.")


def payment_dates(
    settlement: DateScalar,
    maturity: DateScalar,
) -> pd.Series:
    """
    Generate all remaining coupon dates between a settlement date and a maturity date.
    The dates are exclusive for the settlement date and inclusive for the maturity date.
    Coupon payments are made on the 1st of January and July.
    The NTN-F bond is determined by its maturity date.

    Args:
        settlement (DateScalar): The settlement date.
        maturity (DateScalar): The maturity date.

    Returns:
        pd.Series: A Series containing the coupon dates between the settlement
            (exclusive) and maturity (inclusive) dates.

    Examples:

        >>> yd.ntnf.payment_dates("15-05-2024", "01-01-2025")
        0   2024-07-01
        1   2025-01-01
        dtype: datetime64[ns]
    """
    # Validate and normalize dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

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
    settlement: DateScalar,
    maturity: DateScalar,
    adj_payment_dates: bool = False,
) -> pd.DataFrame:
    """
    Generate the cash flows for the NTN-F bond between the settlement (exclusive) and
    maturity dates (inclusive). The cash flows are the coupon payments and the final
    payment at maturity.

    Args:
        settlement (DateScalar): The date (exclusive) for starting the cash flows.
        maturity (DateScalar): The maturity date of the bond.
        adj_payment_dates (bool): If True, adjust the payment dates to the next
            business day.

    Returns:
        pd.DataFrame: DataFrame with columns "PaymentDate" and "CashFlow".

    Examples:
        >>> yd.ntnf.cash_flows("15-05-2024", "01-01-2025")
          PaymentDate    CashFlow
        0  2024-07-01    48.80885
        1  2025-01-01  1048.80885
    """
    # Validate input dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)
    _check_maturity_date(maturity)

    # Get the coupon payment dates between the settlement and maturity dates
    pay_dates = payment_dates(settlement, maturity)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    cf_values = np.where(pay_dates == maturity, FINAL_PMT, COUPON_PMT)

    df = pd.DataFrame(data={"PaymentDate": pay_dates, "CashFlow": cf_values})

    if adj_payment_dates:
        df["PaymentDate"] = bday.offset(df["PaymentDate"], 0)

    return df


def price(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the NTN-F price using Anbima rules, which corresponds to the present
        value of the cash flows discounted at the given yield to maturity rate (YTM).

    Args:
        settlement (DateScalar): The settlement date to calculate the price.
        maturity (DateScalar): The maturity date of the bond.
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
        >>> yd.ntnf.price("05-07-2024", "01-01-2035", 0.11921)
        895.359254
    """
    cf_df = cash_flows(settlement, maturity)
    cf_values = cf_df["CashFlow"]
    bdays = bday.count(settlement, cf_df["PaymentDate"])
    byears = tt.truncate(bdays / 252, 14)
    discount_factors = (1 + rate) ** byears
    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    dcf = (cf_values / discount_factors).round(9)
    # Return the sum of the discounted cash flows truncated as per Anbima rules
    return tt.truncate(dcf.sum(), 6)


def spot_rates(  # noqa
    settlement: DateScalar,
    ltn_maturities: pd.Series,
    ltn_rates: pd.Series,
    ntnf_maturities: pd.Series,
    ntnf_rates: pd.Series,
    show_coupons: bool = False,
) -> pd.DataFrame:
    """
    Calculate the spot rates (zero coupon rates) for NTN-F bonds using the bootstrap
    method.

    The bootstrap method is a process used to determine spot rates from
    the yields of a series of bonds. It involves iteratively solving for
    the spot rates that discount each bond's cash flows to its current
    price. It uses the LTN rates, which are zero coupon bonds, up to the
    last LTN maturity available. For maturities after the last LTN maturity,
    it calculates the spot rates using the bootstrap method.


    Args:
        settlement (DateScalar): The settlement date for the spot rates calculation.
        ltn_maturities (pd.Series): The LTN known maturities.
        ltn_rates (pd.Series): The LTN known rates.
        ntnf_maturities (pd.Series): The NTN-F known maturities.
        ntnf_rates (pd.Series): The NTN-F known rates.
        show_coupons (bool): If True, show also July rates corresponding to the
            coupon payments. Defaults to False.

    Returns:
        pd.DataFrame: DataFrame with columns "BDToMat", "MaturityDate" and "SpotRate".
            "BDToMat" is the business days from the settlement date to the maturities.

    Examples:
        >>> df_ltn = yd.ltn.rates("03-09-2024")
        >>> df_ntnf = yd.ntnf.rates("03-09-2024")
        >>> yd.ntnf.spot_rates(
        ...     settlement="03-09-2024",
        ...     ltn_maturities=df_ltn["MaturityDate"],
        ...     ltn_rates=df_ltn["IndicativeRate"],
        ...     ntnf_maturities=df_ntnf["MaturityDate"],
        ...     ntnf_rates=df_ntnf["IndicativeRate"],
        ... )
          MaturityDate  BDToMat  SpotRate
        0   2025-01-01       83  0.108837
        1   2027-01-01      584  0.119981
        2   2029-01-01     1083  0.122113
        3   2031-01-01     1584  0.122231
        4   2033-01-01     2088  0.121355
        5   2035-01-01     2587  0.121398
    """
    # Process and validate the input data
    settlement = dc.convert_input_dates(settlement)

    # Create flat forward interpolators for LTN and NTN-F rates
    ltn_rate_interpolator = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, ltn_maturities),
        known_rates=ltn_rates,
    )
    ntnf_rate_interpolator = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, ntnf_maturities),
        known_rates=ntnf_rates,
    )

    # Generate all coupon dates up to the last NTN-F maturity date
    all_coupon_dates = payment_dates(settlement, ntnf_maturities.max())

    # Create a DataFrame with all coupon dates and the corresponding YTM
    df = pd.DataFrame(data=all_coupon_dates, columns=["MaturityDate"])
    df["BDToMat"] = bday.count(start=settlement, end=df["MaturityDate"])
    df["BYears"] = df["BDToMat"] / 252
    df["Coupon"] = COUPON_PMT
    df["YTM"] = df["BDToMat"].apply(ntnf_rate_interpolator)

    # The Bootstrap loop to calculate spot rates
    for index, row in df.iterrows():
        if row["MaturityDate"] <= ltn_maturities.max():
            # Use LTN rates for maturities before the last LTN maturity date
            df.at[index, "SpotRate"] = ltn_rate_interpolator(row["BDToMat"])
            continue

        # Calculate the present value of the coupon payments
        cf_dates = payment_dates(settlement, row["MaturityDate"])[:-1]  # noqa
        cf_df = df.query("MaturityDate in @cf_dates").reset_index(drop=True)
        cf_present_value = tt.calculate_present_value(
            cash_flows=cf_df["Coupon"],
            rates=cf_df["SpotRate"],
            periods=cf_df["BDToMat"] / 252,
        )

        bond_price = price(settlement, row["MaturityDate"], row["YTM"])
        price_factor = FINAL_PMT / (bond_price - cf_present_value)
        df.at[index, "SpotRate"] = price_factor ** (1 / row["BYears"]) - 1

    df = df[["MaturityDate", "BDToMat", "SpotRate"]].copy()
    df["SpotRate"] = df["SpotRate"].astype("Float64")

    if not show_coupons:
        df = df.query("MaturityDate in @ntnf_maturities").reset_index(drop=True)

    return df


def di_spreads(date: DateScalar) -> pd.DataFrame:
    """
    Calculates the DI spread for the NTN-F based on ANBIMA's indicative rates.

    This function fetches the indicative rates for the NTN-F bonds and the DI futures
    rates and calculates the spread between these rates in basis points.

    Parameters:
        date (DateScalar): The reference date for the spread calculation.

    Returns:
        pd.DataFrame: DataFrame with columns "MaturityDate", "DISpread".

    Examples:
        >>> yd.ntnf.di_spreads("23-08-2024")
          MaturityDate  DISpread
        0   2025-01-01     -5.38
        1   2027-01-01      4.39
        2   2029-01-01      7.37
        3   2031-01-01     12.58
        4   2033-01-01      7.67
        5   2035-01-01     12.76

    """
    # Fetch DI Spreads for the reference date
    df = tt.pre_spreads(date)
    df = (
        df.query("BondType == 'NTN-F'")
        .sort_values(["MaturityDate"])
        .reset_index(drop=True)
    )
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
    settlement: DateScalar,
    ntnf_maturity: DateScalar,
    ntnf_rate: float,
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
        settlement (DateScalar): The settlement date to calculate the spread.
        ntnf_maturity (DateScalar): The bond maturity date.
        ntnf_rate (float): The yield to maturity (YTM) of the bond.
        di_rates (pd.Series): A Series of DI rates.
        di_expirations (pd.Series): A list or Series of DI expiration dates.
        initial_guess (float, optional): An initial guess for the spread. Defaults to
            None. A good initial guess is the DI gross spread for the bond.

    Returns:
        float: The net DI spread in basis points.

    Examples:
        # Obs: only some of the DI rates will be used in the example.
        >>> exp_dates = pd.to_datetime(["2025-01-01", "2030-01-01", "2035-01-01"])
        >>> di_rates = pd.Series([0.10823, 0.11594, 0.11531])
        >>> di_net_spread(
        ...     settlement="23-08-2024",
        ...     ntnf_maturity="01-01-2035",
        ...     ntnf_rate=0.116586,
        ...     di_expirations=exp_dates,
        ...     di_rates=di_rates,
        ... )
        12.13
    """
    # Create an interpolator for the DI rates using the flat-forward method
    settlement = dc.convert_input_dates(settlement)
    ntnf_maturity = dc.convert_input_dates(ntnf_maturity)

    ff_interpolator = ip.Interpolator(
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
    df = cash_flows(settlement, ntnf_maturity).reset_index()
    df["BDToMat"] = bday.count(settlement, df["PaymentDate"])

    byears = bday.count(settlement, df["PaymentDate"]) / 252
    di_interp = df["BDToMat"].apply(ff_interpolator)
    bond_price = price(settlement, ntnf_maturity, ntnf_rate)
    bond_cash_flows = df["CashFlow"]

    def price_difference(p):
        # Difference between the bond's price and its discounted cash flows
        return (bond_cash_flows / (1 + di_interp + p) ** byears).sum() - bond_price

    # Solve for the spread that zeroes the price difference using the bisection method
    p_solution = _solve_spread(price_difference, initial_guess)
    # Convert the solution to basis points (bps) and round to two decimal places
    return round((p_solution * 10_000), 2)


def premium(
    settlement: DateScalar,
    ntnf_maturity: DateScalar,
    ntnf_rate: float,
    di_expirations: pd.Series,
    di_rates: pd.Series,
) -> float:
    """
    Calculate the premium of an NTN-F bond over DI rates.

    This function computes the premium of an NTN-F bond by comparing its implied
    discount factor with that of the DI curve. It determines the net premium based
    on the difference between the discount factors of the bond's yield-to-maturity
    (YTM) and the interpolated DI rates.

    Args:
        settlement (DateScalar): The settlement date to calculate the premium.
        ntnf_maturity (DateScalar): The maturity date of the NTN-F bond.
        ntnf_rate (float): The yield to maturity (YTM) of the NTN-F bond.
        di_expirations (pd.Series): Series containing the expiration dates for DI rates.
        di_rates (pd.Series): Series containing the DI rates corresponding to
            the expiration dates.

    Returns:
        float: The premium of the NTN-F bond over the DI curve, expressed as a
        factor.

    Examples:
        >>> # Obs: only some of the DI rates will be used in the example.
        >>> exp_dates = pd.to_datetime(["2025-01-01", "2030-01-01", "2035-01-01"])
        >>> di_rates = pd.Series([0.10823, 0.11594, 0.11531])
        >>> premium(
        ...     settlement="23-08-2024",
        ...     ntnf_maturity="01-01-2035",
        ...     ntnf_rate=0.116586,
        ...     di_expirations=exp_dates,
        ...     di_rates=di_rates,
        ... )
        1.0099602136954626

    Notes:
        - The function adjusts coupon payment dates to business days and calculates
          the present value of cash flows for the NTN-F bond using DI rates.

    """
    ntnf_maturity = dc.convert_input_dates(ntnf_maturity)
    settlement = dc.convert_input_dates(settlement)

    df = cash_flows(settlement, ntnf_maturity, adj_payment_dates=True)
    df["BDToMat"] = bday.count(settlement, df["PaymentDate"])
    df["BYears"] = df["BDToMat"] / 252

    ff_interpolator = ip.Interpolator(
        "flat_forward",
        bday.count(settlement, di_expirations),
        di_rates,
    )

    df["DIRate"] = df["BDToMat"].apply(ff_interpolator)

    # Calculate the present value of the cash flows using the DI rate
    bond_price = tt.calculate_present_value(
        cash_flows=df["CashFlow"],
        rates=df["DIRate"],
        periods=df["BDToMat"] / 252,
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
    return float(premium_np)


def historical_premium(
    date: DateScalar,
    maturity: DateScalar,
) -> float:
    date = dc.convert_input_dates(date)
    maturity = dc.convert_input_dates(maturity)

    df_ntnf = rates(date)
    if df_ntnf.empty:
        return float("NaN")

    ntnf_ytms = df_ntnf.query("MaturityDate == @maturity")["IndicativeRate"]
    if ntnf_ytms.empty:
        return float("NaN")
    ntnf_ytm = float(ntnf_ytms.iloc[0])

    df = cash_flows(date, maturity, adj_payment_dates=True)
    df["BDToMat"] = bday.count(date, df["PaymentDate"])
    df["BYears"] = df["BDToMat"] / 252
    df["ReferenceDate"] = date

    dif = di.DIFutures()  # Instantiate the DI Futures class
    df["DIRate"] = dif.interpolate_rates(
        dates=df["ReferenceDate"],
        expirations=df["PaymentDate"],
        extrapolate=False,
    )

    # Calculate the present value of the cash flows using the DI rate
    bond_price = tt.calculate_present_value(
        cash_flows=df["CashFlow"],
        rates=df["DIRate"],
        periods=df["BDToMat"] / 252,
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


def duration(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the Macaulay duration for an NTN-F bond.

    The Macaulay duration is a weighted average of the times until each payment is
    received, with the weights proportional to the present value of the cash flows.
    It measures the bond's sensitivity to interest rate changes.

    Args:
        settlement (DateScalar): The settlement date to calculate the duration.
        maturity (DateScalar): The maturity date of the bond.
        rate (float): The yield to maturity (YTM) used to discount the cash flows.

    Returns:
        float: The Macaulay duration in years.

    Examples:
        >>> yd.ntnf.duration("02-09-2024", "01-01-2035", 0.121785)
        6.32854218039796
    """
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

    df = cash_flows(settlement, maturity)
    df["BY"] = bday.count(settlement, df["PaymentDate"]) / 252
    df["DCF"] = df["CashFlow"] / (1 + rate) ** df["BY"]
    np_duration = (df["DCF"] * df["BY"]).sum() / df["DCF"].sum()
    return float(np_duration)
