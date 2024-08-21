import numpy as np
import pandas as pd

from .. import bday
from .. import date_converter as dc
from .. import fetchers as ft
from .. import interpolator as it
from ..fetchers import anbima as an
from . import ltn
from . import utils as ut

"""
Constants calculated as per Anbima Rules and in base 100
COUPON_RATE = (0.06 + 1) ** 0.5 - 1  # 6% annual rate compounded semi-annually
COUPON_PMT = round(100 * COUPON_RATE, 6) -> 2.956301
FINAL_PMT = principal + last coupon payment = 100 + 2.956301
"""
COUPON_DAY = 15
COUPON_MONTHS = {2, 5, 8, 11}
COUPON_PMT = 2.956301
FINAL_PMT = 102.956301


def anbima_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-B Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return an.anbima_data(reference_date, "NTN-B")


def indicative_rates(reference_date: str | pd.Timestamp) -> pd.Series:
    """
    Fetch NTN-B Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.Series: A Series containing the rates indexed by maturity date.
    """
    return an.get_anbima_rates(reference_date, "NTN-B")


def maturities(reference_date: str | pd.Timestamp) -> list[pd.Timestamp]:
    """
    Fetch the bond maturities available for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        list[pd.Timestamp]: A list of bond maturities available for the reference
            date.
    """
    rates = indicative_rates(reference_date)
    return rates.index.to_list()


def _coupon_dates_map(
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
) -> pd.Series:
    """
    Generate a map of all possible coupon dates between the start and end dates.
    The dates are inclusive. Coupon payments are made on the 15th of February, May,
    August, and November (15-02, 15-05, 15-08, and 15-11 of each year).

    Args:
        start (str | pd.Timestamp): The start date.
        end (str | pd.Timestamp): The end date.

    Returns:
        pd.Series: Series of coupon dates within the specified range.
    """
    # Validate and normalize dates
    start = dc.convert_date(start)
    end = dc.convert_date(end)

    # Initialize the first coupon date based on the reference date
    reference_year = start.year
    first_coupon_date = pd.Timestamp(f"{reference_year}-02-01")

    # Generate coupon dates
    dates = pd.date_range(start=first_coupon_date, end=end, freq="3MS")

    # Offset dates by 14 in order to have day 15 of the month
    dates += pd.Timedelta(days=14)

    # First coupon date must be after the reference date
    dates = dates[dates >= start]
    return pd.Series(dates).reset_index(drop=True)


def coupon_dates(
    start: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
) -> pd.Series:
    """
    Generate all remaining coupon dates between a given date and the maturity date.
    The dates are inclusive. Coupon payments are made on the 15th of February, May,
    August, and November (15-02, 15-05, 15-08, and 15-11 of each year). The NTN-B
    bond is determined by its maturity date.

    Args:
        start_date (str | pd.Timestamp): The date to start generating coupon dates.
        maturity_date (str | pd.Timestamp): The maturity date.

    Returns:
        list[pd.Timestamp]: List of coupon dates between start and maturity dates.
    """
    # Validate and normalize dates
    start = dc.convert_date(start)
    maturity = dc.convert_date(maturity)

    # Check if maturity date is after the start date
    if maturity < start:
        raise ValueError("Maturity date must be after the start date.")

    # Check if the maturity date is a valid NTN-B maturity date
    if maturity.day != COUPON_DAY or maturity.month not in COUPON_MONTHS:
        raise ValueError("NTN-B maturity must be 15/02, 15/05, 15/08, or 15/11.")

    # Initialize loop variables
    cp_date = maturity
    cp_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while cp_date >= start:
        cp_dates.append(cp_date)
        # Move the coupon date back 6 months
        cp_date -= pd.DateOffset(months=6)

    return pd.Series(cp_dates).sort_values(ignore_index=True)


def quotation(
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
    rate: float,
) -> float:
    """
    Calculate the NTN-B quotation in base 100 using Anbima rules.

    Args:
        settlement (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-B.

    Returns:
        float: The NTN-B quotation truncated to 4 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - The semi-annual coupon is set to 2.956301, which represents a 6% annual
          coupon rate compounded semi-annually and rounded to 6 decimal places as per
          Anbima rules.

    Examples:
        >>> ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)
        99.3651
        >>> ntnb.quotation("31-05-2024", "15-08-2060", 0.061878)
        99.5341
    """
    # Validate and normalize dates
    settlement = dc.convert_date(settlement)
    maturity = dc.convert_date(maturity)

    # Get the coupon dates between the settlement and maturity dates
    payment_dates = coupon_dates(settlement, maturity)

    # Coupon payment dates must be after the settlement date
    payment_dates = payment_dates[payment_dates > settlement]

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement, payment_dates)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    cash_flows = np.where(payment_dates == maturity, FINAL_PMT, COUPON_PMT)

    # Calculate the number of periods truncated as per Anbima rules
    num_of_years = ut.truncate(bdays / 252, 14)

    discount_factor = (1 + rate) ** num_of_years

    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    discounted_cash_flows = (cash_flows / discount_factor).round(10)

    # Return the quotation (the dcf sum) truncated as per Anbima rules
    return ut.truncate(discounted_cash_flows.sum(), 4)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calculate the NTN-B price using Anbima rules.

    Args:
        vna (float): The nominal value of the NTN-B bond.
        quotation (float): The NTN-B quotation in base 100.

    Returns:
        float: The NTN-B price truncated to 6 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> ntnb.price(4299.160173, 99.3651)
        4271.864805
    """
    return ut.truncate(vna * quotation / 100, 6)


def _calculate_coupons_pv(
    bootstrap_df: pd.DataFrame,
    settlement: pd.Timestamp,
    maturity: pd.Timestamp,
) -> float:
    # Create a subset DataFrame with only the coupon payments (without last payment)
    cp_dates_wo_last = coupon_dates(settlement, maturity)[:-1]  # noqa
    if len(cp_dates_wo_last) == 0:
        return 0

    df_coupons = bootstrap_df.query("MaturityDate in @cp_dates_wo_last").copy()
    df_coupons["Coupon"] = COUPON_PMT

    # Calculate the present value of the coupon payments
    pv = ut.calculate_present_value(
        cash_flows=df_coupons["Coupon"],
        rates=df_coupons["SpotRate"],
        periods=df_coupons["BDays"] / 252,
    )
    return pv


def spot_rates(
    settlement: str | pd.Timestamp,
    rates: list[float] | pd.Series,
    maturities: list[str | pd.Timestamp],
) -> pd.DataFrame:
    """
    Calculate the spot rates for NTN-B bonds using the bootstrap method.

    The bootstrap method is a process used to determine spot rates from
    the yields of a series of bonds. It involves iteratively solving for
    the spot rates that discount each bond's cash flows to its current
    price.


    Args:
        settlement_date (str | pd.Timestamp): The reference date for settlement.
        ytm_rates (pd.Series): Series of yield to maturity rates indexed by the
            maturity dates of the bonds.

    Returns:
        pd.DataFrame: DataFrame containing the maturity dates and corresponding real
            spot rates.

    Notes:
        The calculation of the spot rates for NTN-B bonds considers the following steps:
            - Map all all possible payment dates up to the longest maturity date.
            - Interpolate the YTM rates in the intermediate payment dates.
            - Calculate the NTN-B quotation for each maturity date.
            - Calculate the real spot rates for each maturity date.
    """
    # Process and validate the input data
    settlement = dc.convert_date(settlement)
    maturities = pd.to_datetime(maturities, errors="coerce", dayfirst=True)

    # Create the interpolator object
    ytm_rate_interpolator = it.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, maturities),
        known_rates=rates,
    )

    last_ntnb = maturities.max()

    # Generate coupon dates up to the longest maturity date
    all_coupon_dates = _coupon_dates_map(start=settlement, end=last_ntnb)

    # Create a DataFrame with all coupon dates and the corresponding YTM
    df_spot = pd.DataFrame(data=all_coupon_dates, columns=["MaturityDate"])
    df_spot["BDays"] = bday.count(settlement, df_spot["MaturityDate"])
    df_spot["YTM"] = df_spot["BDays"].apply(ytm_rate_interpolator)

    # The Bootstrap loop to calculate spot rates
    for index in df_spot.index:
        # Get the row values using the index
        maturity = df_spot.at[index, "MaturityDate"]
        bdays = df_spot.at[index, "BDays"]
        ytm = df_spot.at[index, "YTM"]

        # Get the coupon dates for the bond without the last one (principal + coupon)
        cp_dates = coupon_dates(settlement, maturity)

        # If there is only one coupon date and this date is the first maturity date
        # of an existing bond, the ytm rate is also a spot rate.
        if len(cp_dates) == 1 and cp_dates[0] == rates.index[0]:
            df_spot.at[index, "SpotRate"] = ytm
            continue

        # Calculate the real spot rate for the bond
        coupons_pv = _calculate_coupons_pv(df_spot, settlement, maturity)
        bond_price = quotation(settlement, maturity, ytm)
        spot_rate = (FINAL_PMT / (bond_price - coupons_pv)) ** (252 / bdays) - 1
        df_spot.at[index, "SpotRate"] = spot_rate

    df_spot.drop(columns=["BDays"], inplace=True)
    # Return the result without the intermediate coupon dates (virtual bonds)
    existing_maturities = rates.index  # noqa
    return df_spot.query("MaturityDate in @existing_maturities").reset_index(drop=True)


def _get_nir_df(reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the Nominal Interest Rate (NIR) data for NTN-B bonds.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame containing the NIR data for NTN-B bonds.
    """
    df = ft.futures(contract_code="DI1", reference_date=reference_date)
    if "CurrentRate" in df.columns:
        df = df.rename(columns={"CurrentRate": "NIR_DI"})
        keep_cols = [
            "TradeDate",
            "TradeTime",
            "TickerSymbol",
            "ExpirationDate",
            "BDaysToExp",
            "NIR_DI",
        ]
    elif "SettlementRate" in df.columns:
        df = df.rename(columns={"SettlementRate": "NIR_DI"})
        keep_cols = [
            "TradeDate",
            "TickerSymbol",
            "ExpirationDate",
            "BDaysToExp",
            "NIR_DI",
        ]
    else:
        raise ValueError("DI data not available.")

    df = df[keep_cols].dropna(subset=["NIR_DI"])

    # Add DI spreads for prefixed bonds (LTN) and adjust NIR
    today = pd.Timestamp.today().normalize()
    anbima_date = reference_date
    if reference_date == today:
        # If the reference date is today, use the previous business day
        anbima_date = bday.offset(reference_date, -1)

    df_ltn = ltn.di_spreads(reference_date=anbima_date).reset_index()
    df_ltn["MaturityDate"] = bday.offset(df_ltn["MaturityDate"], 0)
    df_ltn["DISpread"] /= 10_000  # Remove BPS (basis points) from the spread

    df = pd.merge_asof(df, df_ltn, left_on="ExpirationDate", right_on="MaturityDate")
    df["NIR_PRE"] = df["NIR_DI"] + df["DISpread"]

    return df


def bei_rates(
    reference_date: str | pd.Timestamp,
    settlement: str | pd.Timestamp,
    rates: list[float] | pd.Series,
    maturities: list[str | pd.Timestamp] | pd.Series,
) -> pd.DataFrame:
    """
    Calculate the Breakeven Inflation (BEI) for NTN-B bonds based on nominal and real
    interest rates.

    Args:
        reference_date (str or pd.Timestamp): The reference date for fetching data and
            performing calculations.
        settlement (str or pd.Timestamp): The settlement date for the bonds.
        rates (pd.Series): A series of Yield to Maturity (YTM) rates corresponding
            to the maturity dates of the bonds indexed by the maturity dates.

    Returns:
        pd.DataFrame: DataFrame containing the breakeven inflation rates.

    Returned columns:
        - MaturityDate: Maturity date of the bonds
        - BDays: Number of business days from the settlement date to the maturity.
        - YTM: Yield to Maturity rate for the bonds
        - RIR: Real Interest Rate for the bonds based on the spot rates.
        - NIR_DI: Nominal Interest Rate based on DI Futures.
        - NIR_PRE: Nominal Interest Rate for the prefixed bonds.
        - BEI_DI: Breakeven Inflation Rate calculated with DI Futures.
        - BEI_PRE: Breakeven Inflation Rate calculated with prefixed bonds.
    """
    # Normalize input dates
    reference_date = dc.convert_date(reference_date)
    settlement = dc.convert_date(settlement)
    maturities = pd.to_datetime(maturities, errors="coerce", dayfirst=True)

    # Fetch Nominal Interest Rate (NIR) data
    df_nir = _get_nir_df(reference_date)

    ytm_interplator = it.Interpolator(
        method="flat_forward",
        known_bdays=df_nir["BDaysToExp"],
        known_rates=df_nir["NIR_DI"],
    )
    # Calculate Real Interest Rate (RIR)
    df = spot_rates(settlement, rates, maturities)
    df = df.rename(columns={"SpotRate": "RIR"})
    df["BDays"] = bday.count(reference_date, df["MaturityDate"])
    df["NIR_DI"] = df["BDays"].apply(ytm_interplator)

    # Calculate Breakeven Inflation Rate (BEI)
    df["BEI_DI"] = ((df["NIR_DI"] + 1) / (df["RIR"] + 1)) - 1

    # Adjust BEI for DI spread in prefixed bonds
    ytm_interplator = it.Interpolator(
        method="flat_forward",
        known_bdays=df_nir["BDaysToExp"],
        known_rates=df_nir["NIR_PRE"],
    )
    df["NIR_PRE"] = df["BDays"].apply(ytm_interplator)
    df["BEI_PRE"] = ((df["NIR_PRE"] + 1) / (df["RIR"] + 1)) - 1

    cols_reordered = [
        "MaturityDate",
        "BDays",
        "YTM",
        "RIR",
        "NIR_DI",
        "NIR_PRE",
        "BEI_DI",
        "BEI_PRE",
    ]
    return df[cols_reordered].copy()
