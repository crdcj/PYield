import numpy as np
import pandas as pd

from . import bday
from . import date_validator as dv
from . import interpolators as ip
from .fetchers.anbima import anbima
from .fetchers.futures import futures
from .spreads import spread

# 6% per year compounded semi-annually and rounded to 8 decimal places
COUPON = 0.02956301  # round(((0.06 + 1) ** 0.5 - 1), 8)


def data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-B Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return anbima(bond_type="NTN-B", reference_date=reference_date)


def ytm_rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-B Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the maturity dates and corresponding rates.
    """
    df = anbima("NTN-B", reference_date)

    # Keep only the relevant columns for the output
    keep_columns = ["ReferenceDate", "BondType", "MaturityDate", "IndicativeRate"]
    df = df[keep_columns].copy()

    # Rename IndicativeRate to YTM for consistency
    return df.rename(columns={"IndicativeRate": "YTM"})


def _truncate(value, decimal_places):
    """
    Truncate a float or a Pandas Series to the specified decimal place.

    Args:
        value (float or pandas.Series): The value(s) to be truncated.
        decimal_places (int): The number of decimal places to truncate to.

    Returns:
        float or pandas.Series: The truncated value(s).
    """
    factor = 10**decimal_places
    return np.trunc(value * factor) / factor


def coupon_dates_map(
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
    start = dv.normalize_date(start)
    end = dv.normalize_date(end)

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
    start_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
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
        pd.Series: Series of coupon dates within the specified range.
    """
    # Validate and normalize dates
    start_date = dv.normalize_date(start_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Initialize loop variables
    coupon_date = maturity_date
    coupon_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while coupon_date >= start_date:
        coupon_dates.append(coupon_date)
        # Move the coupon date back 6 months
        coupon_date -= pd.DateOffset(months=6)

    # Return the coupon dates as a sorted Series
    return pd.Series(coupon_dates).sort_values(ignore_index=True)


def quotation(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
    discount_rate: float,
) -> float:
    """
    Calculate the NTN-B quotation in base 100 using Anbima rules.

    Args:
        settlement_date (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity_date (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        discount_rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-B.

    Returns:
        float: The NTN-B quotation truncated to 4 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - The semi-annual coupon is set to 2.956301, which represents a 6% annual
          coupon rate compounded semi-annually and rounded to 6 decimal places as per
          Anbima rules.

    Examples:
        >>> quotation("31-05-2024", "15-05-2035", 0.061490)
        99.3651
        >>> quotation("31-05-2024", "15-08-2060", 0.061878)
        99.5341
    """
    # Semi-annual coupon values are in base 100 and rounded to 6 decimal places
    INTER_PMT = round(100 * COUPON, 6)
    FINAL_PMT = round(100 + INTER_PMT, 6)

    # Validate and normalize dates
    settlement_date = dv.normalize_date(settlement_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Create a Series with the coupon dates
    payment_dates = pd.Series(coupon_dates(settlement_date, maturity_date))

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement_date, payment_dates)

    # Set the cash flow at maturity to 100, otherwise set it to the coupon
    cf = np.where(payment_dates == maturity_date, FINAL_PMT, INTER_PMT)

    # Calculate the number of periods truncated to 14 decimal places
    n = _truncate(bdays / 252, 14)

    # Calculate the present value of each cash flow (DCF) rounded to 10 decimal places
    dcf = (cf / (1 + discount_rate) ** n).round(10)

    # Return the quotation (the dcf sum) truncated to 4 decimal places
    return _truncate(dcf.sum(), 4)


def _prepare_interpolation_data(
    reference_date: pd.Timestamp, maturity_dates: pd.Series, rates: pd.Series
) -> tuple:
    """
    Prepare the data needed for interpolation by sorting the YTM rates by the number of
    business days.

    Args:
        reference_date (str | pd.Timestamp): The reference date for calculating the
            number of business days.
        maturity_dates (pd.Series): Series of maturity dates for the bonds.
        rates (pd.Series): Series of rates corresponding to the maturity dates.

    Returns:
        tuple: Two lists containing the ordered business days and YTM rates.
    """
    bdays = bday.count(reference_date, maturity_dates)
    df = pd.DataFrame({"BDays": bdays, "Rates": rates})
    df.sort_values(by="BDays", ignore_index=True, inplace=True)
    ordered_bdays = df["BDays"].to_list()
    ordered_rates = df["Rates"].to_list()
    return ordered_bdays, ordered_rates


def spot_rates(
    settlement_date: str | pd.Timestamp,
    maturity_dates: pd.Series,
    ytm_rates: pd.Series,
) -> pd.DataFrame:
    """
    Calculate the spot rates for NTN-B bonds based on given settlement date, maturity
    dates, and YTM rates.

    Args:
        settlement_date (str | pd.Timestamp): The reference date for settlement.
        maturity_dates (pd.Series): Series of maturity dates for the bonds.
        ytm_rates (pd.Series): Series of Yield to Maturity rates corresponding to the
            maturity dates.

    Returns:
        pd.DataFrame: DataFrame containing the maturity dates and corresponding spot
            rates.

    Notes:
        The calculation of the spot rates for NTN-B bonds considers the following steps:
            - Map all all possible payment dates up to the longest maturity date.
            - Interpolate the YTM rates in the intermediate payment dates.
            - Calculate the NTN-B quotation for each maturity date.
            - Calculate the spot rates for each maturity date.
    """
    # COUPON = (1.06) ** 0.5 - 1  # Coupon without rounding
    # Validate and normalize the settlement date
    settlement_date = dv.normalize_date(settlement_date)

    # Prepare the data for interpolation
    ordered_bdays, ordered_ytms = _prepare_interpolation_data(
        settlement_date, maturity_dates, ytm_rates
    )
    # Generate coupon dates and initialize the main DataFrame
    longest_ntnb = maturity_dates.max()
    coupon_dates_all = coupon_dates_map(settlement_date, longest_ntnb)
    df = pd.DataFrame(coupon_dates_all, columns=["MaturityDate"])

    # Add auxiliary columns for calculations
    df["BDays"] = bday.count(settlement_date, df["MaturityDate"])
    df["YTM"] = 0.0
    df["RSR"] = 0.0

    # Main loop to calculate spot rates
    for index in df.index:
        maturity_date = df.at[index, "MaturityDate"]
        # Get the coupon dates for the bond without the last one (principal + coupon)
        coupon_dates_wo_last = coupon_dates(settlement_date, maturity_date)[:-1]  # noqa

        # Create a local DataFrame with a subset of the main DataFrame
        dfl = df.query("MaturityDate in @coupon_dates_wo_last").reset_index(drop=True)

        # Create the Series that will be used to calculate the discounted cash flows
        cfs = pd.Series(COUPON, index=dfl.index)
        spot_rates = dfl["RSR"]
        periods = dfl["BDays"] / 252

        # Calculate the present value of the cash flows (discounted cash flows)
        dcfs = cfs / (1 + spot_rates) ** periods

        # Interpolate YTM and calculate spot rate
        bd = df.at[index, "BDays"]
        ytm = ip.find_and_interpolate_flat_forward(bd, ordered_bdays, ordered_ytms)
        ntnb_quotation = quotation(settlement_date, maturity_date, ytm) / 100
        spot_rate = ((COUPON + 1) / (ntnb_quotation - dcfs.sum())) ** (252 / bd) - 1

        # Update DataFrame with calculated values
        df.at[index, "RSR"] = spot_rate
        df.at[index, "YTM"] = ytm

    # Drop the BDays column, remove intermediate cupon dates and reset the index.
    return (
        df.drop(columns=["BDays"])
        .query("MaturityDate in @maturity_dates")
        .reset_index(drop=True)
    )


def _get_nsr_df(reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the Nominal Interest Rate (NIR) data for NTN-B bonds.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame containing the NIR data for NTN-B bonds.
    """
    df = futures(contract_code="DI1", reference_date=reference_date)
    if "CurrentRate" in df.columns:
        df = df.rename(columns={"CurrentRate": "NSR_DI"})
        keep_cols = [
            "TradeDate",
            "TradeTime",
            "TickerSymbol",
            "ExpirationDate",
            "BDaysToExp",
            "CurrentAskRate",
            "CurrentBidRate",
            "NSR_DI",
        ]
    elif "SettlementRate" in df.columns:
        df = df.rename(columns={"SettlementRate": "NSR_DI"})
        keep_cols = [
            "TradeDate",
            "TickerSymbol",
            "ExpirationDate",
            "BDaysToExp",
            "NSR_DI",
        ]
    else:
        raise ValueError("NIR data not found in the DataFrame.")

    df = df[keep_cols].dropna(subset=["NSR_DI"])

    # Add DI spreads for prefixed bonds (LTN) and adjust NIR
    today = pd.Timestamp.today().normalize()
    anbima_date = reference_date
    if reference_date == today:
        # If the reference date is today, use the previous business day
        anbima_date = bday.offset(reference_date, -1)
    df_pre = spread(spread_type="DI_PRE", reference_date=anbima_date)
    df_pre.query("BondType == 'LTN'", inplace=True)
    df_pre["MaturityDate"] = bday.offset(df_pre["MaturityDate"], 0)
    df_pre["DISpread"] /= 10_000  # Remove BPS (basis points) from the spread
    df_pre.drop(columns=["BondType"], inplace=True)

    df = pd.merge_asof(df, df_pre, left_on="ExpirationDate", right_on="MaturityDate")
    df["NSR_PRE"] = df["NSR_DI"] + df["DISpread"]

    return df


def bei_rates(
    reference_date: str | pd.Timestamp,
    settlement_date: str | pd.Timestamp,
    maturity_dates: pd.Series,
    ytm_rates: pd.Series,
) -> pd.DataFrame:
    """
    Calculate the Breakeven Inflation (BEI) for NTN-B bonds based on nominal and real
    interest rates.

    Args:
        reference_date (str or pd.Timestamp): The reference date for fetching data and
            performing calculations.
        settlement_date (str or pd.Timestamp): The settlement date for the bonds.
        maturity_dates (pd.Series): A series of maturity dates for the bonds.
        ytm_rates (pd.Series): A series of Yield to Maturity (YTM) rates corresponding
            to the maturity dates.

    Returns:
        pd.DataFrame: DataFrame containing the breakeven inflation rates.

    Returned columns:
        - MaturityDate: Maturity date of the bond.
        - BDays: Number of business days from the settlement date to the maturity.
        - YTM: Yield to Maturity rate for the bond.
        - RSR: Real Spot Rate for the bond.
        - NSR_DI: Nominal Spot Rate for the bond.
        - NSR_PRE: Nominal Spot Rate for the bond with DI spread.
        - BIR_DI: Breakeven Inflation Rate for the bond.
        - BIR_PRE: Breakeven Inflation Rate for the bond adjusted for DI spread.
    """
    # Normalize input dates
    reference_date = dv.normalize_date(reference_date)
    settlement_date = dv.normalize_date(settlement_date)

    # Fetch Nominal Spot Rate (NSR) data
    df_nsr = _get_nsr_df(reference_date)
    known_bdays = df_nsr["BDaysToExp"].to_list()
    known_rates = df_nsr["NSR_DI"].to_list()

    # Calculate Real Spot Rate (RSR)
    df = spot_rates(settlement_date, maturity_dates, ytm_rates)
    df = df.rename(columns={"RSR": "RSR"})
    df["BDays"] = bday.count(reference_date, df["MaturityDate"])
    df["NSR_DI"] = df["BDays"].apply(
        lambda x: ip.find_and_interpolate_flat_forward(x, known_bdays, known_rates)
    )

    # Calculate Breakeven Inflation Rate (BIR)
    df["BIR_DI"] = ((df["NSR_DI"] + 1) / (df["RSR"] + 1)) - 1

    # Adjust BEI for DI spread in prefixed bonds
    known_rates = df_nsr["NSR_PRE"].to_list()
    df["NSR_PRE"] = df["BDays"].apply(
        lambda x: ip.find_and_interpolate_flat_forward(x, known_bdays, known_rates)
    )
    df["BIR_PRE"] = ((df["NSR_PRE"] + 1) / (df["RSR"] + 1)) - 1

    cols_reordered = [
        "MaturityDate",
        "BDays",
        "YTM",
        "RSR",
        "NSR_DI",
        "NSR_PRE",
        "BIR_DI",
        "BIR_PRE",
    ]
    return df[cols_reordered].copy()
