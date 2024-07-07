import numpy as np
import pandas as pd

from .. import bday
from .. import date_validator as dv
from ..fetchers.anbima import anbima
from ..fetchers.futures import futures
from ..interpolator import Interpolator
from ..spreads import spread
from .utils import truncate

# Constants for NTN-B bonds
COUPON_DAY = 15
COUPON_MONTHS = [2, 5, 8, 11]
COUPON_RATE = (0.06 + 1) ** 0.5 - 1  # 6% annual rate compounded semi-annually
# Semi-annual payments are in base 100 and rounded using Anbima rules
COUPON_PMT = round(100 * COUPON_RATE, 6)
FINAL_PMT = 100 + COUPON_PMT


def anbima_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-B Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return anbima(bond_type="NTN-B", reference_date=reference_date)


def anbima_rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-B Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the maturity dates and corresponding rates.
    """
    df = anbima_data(reference_date)

    # Keep only the relevant columns for the output
    keep_columns = ["ReferenceDate", "BondType", "MaturityDate", "IndicativeRate"]
    return df[keep_columns].copy()


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
        list[pd.Timestamp]: List of coupon dates between start and maturity dates.
    """
    # Validate and normalize dates
    start_date = dv.normalize_date(start_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Check if maturity date is after the start date
    if maturity_date < start_date:
        raise ValueError("Maturity date must be after the start date.")

    # Check if the maturity date is 15
    if maturity_date.day != COUPON_DAY:
        raise ValueError("Maturity date must be the 15th of a month.")

    # Check if month is February, May, August, or November
    if maturity_date.month not in COUPON_MONTHS:
        raise ValueError("Maturity months must be February, May, August, or November.")

    # Initialize loop variables
    cp_date = maturity_date
    cp_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while cp_date >= start_date:
        cp_dates.append(cp_date)
        # Move the coupon date back 6 months
        cp_date -= pd.DateOffset(months=6)

    return pd.Series(cp_dates).sort_values().reset_index(drop=True)


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
    # Validate and normalize dates
    settlement_date = dv.normalize_date(settlement_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Get the coupon dates between the settlement and maturity dates
    payment_dates = coupon_dates(settlement_date, maturity_date)

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement_date, payment_dates)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    cash_flows = np.where(payment_dates == maturity_date, FINAL_PMT, COUPON_PMT)

    # Calculate the number of periods truncated as per Anbima rules
    num_periods = truncate(bdays / 252, 14)

    discount_factor = (1 + discount_rate) ** num_periods

    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    discounted_cash_flows = (cash_flows / discount_factor).round(10)

    # Return the quotation (the dcf sum) truncated as per Anbima rules
    return truncate(discounted_cash_flows.sum(), 4)


def _validate_and_process_inputs(settlement_date, maturity_dates, ytm_rates) -> tuple:
    """Validate and process the inputs for the bootstrap process."""

    if len(maturity_dates) != len(ytm_rates):
        raise ValueError("maturity_dates and ytm_rates must have the same length.")

    df = pd.DataFrame({"maturity": maturity_dates, "ytm": ytm_rates})
    df = df.dropna().drop_duplicates(subset="maturity").sort_values("maturity")

    settlement_date = dv.normalize_date(settlement_date)
    maturity_dates = df["maturity"]
    ytm_rates = df["ytm"]

    return settlement_date, maturity_dates, ytm_rates


def _calculate_discounted_cash_flow(df) -> np.float64:
    if df.empty:
        return np.float64(0)
    # Create the Series that will be used to calculate the discounted cash flows
    cash_flows = pd.Series(COUPON_RATE, index=df.index)
    spot_rates = df["RSR"]
    periods = df["BDays"] / 252

    # Calculate the present value of the cash flows (discounted cash flows)
    discounted_cash_flows = cash_flows / (1 + spot_rates) ** periods

    return discounted_cash_flows.sum()


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
        maturity_dates (pd.Series): Series of maturity dates for the bonds. ytm_rates
        (pd.Series): Series of Yield to Maturity rates corresponding to the
            maturity dates.

    Returns:
        pd.DataFrame: DataFrame containing the maturity dates and corresponding real
            spot rates (RSR).

    Notes:
        The calculation of the spot rates for NTN-B bonds considers the following steps:
            - Map all all possible payment dates up to the longest maturity date.
            - Interpolate the YTM rates in the intermediate payment dates.
            - Calculate the NTN-B quotation for each maturity date.
            - Calculate the real spot rates (RSR) for each maturity date.
    """
    # Validate and process the inputs
    settlement_date, maturity_dates, ytm_rates = _validate_and_process_inputs(
        settlement_date, maturity_dates, ytm_rates
    )

    # Create the interpolator object
    flat_fwd = Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement_date, maturity_dates),
        known_rates=ytm_rates,
    )

    # Generate coupon dates up to the longest maturity date
    all_coupon_dates = coupon_dates_map(
        start=settlement_date,
        end=maturity_dates.max(),
    )

    # Create a DataFrame with all coupon dates
    df = pd.DataFrame(all_coupon_dates, columns=["MaturityDate"])

    # Add auxiliary columns for calculations
    df["BDays"] = bday.count(settlement_date, df["MaturityDate"])
    df["YTM"] = df["BDays"].apply(flat_fwd.interpolate)
    df["RSR"] = 0.0

    # Main loop to calculate spot rates
    for index in df.index:
        # Get the row values using the index
        maturity_date = df.at[index, "MaturityDate"]
        bd = df.at[index, "BDays"]
        ytm = df.at[index, "YTM"]

        # Get the coupon dates for the bond without the last one (principal + coupon)
        cp_dates = coupon_dates(settlement_date, maturity_date)

        # If there is only one coupon date and it is the first maturity date,
        # the ytm rate is also a spot rate.
        if len(cp_dates) == 1 and cp_dates[0] == maturity_dates[0]:
            df.at[index, "RSR"] = ytm
            continue

        # Create a subset DataFrame with the coupon dates without the last one
        cp_dates_wo_last = cp_dates[:-1]  # noqa
        df_subset = df.query("MaturityDate in @cp_dates_wo_last").reset_index(drop=True)

        # Calculate the present value of the cash flows (discounted cash flows)
        dcf = _calculate_discounted_cash_flow(df_subset)

        # Calculate the real spot rate (RSR) for the bond
        q = quotation(settlement_date, maturity_date, ytm) / 100
        df.at[index, "RSR"] = ((COUPON_RATE + 1) / (q - dcf)) ** (252 / bd) - 1

    # Drop the BDays column, remove intermediate cupon dates and reset the index.
    return (
        df.drop(columns=["BDays"])
        .query("MaturityDate in @maturity_dates")
        .reset_index(drop=True)
    )


def anbima_spot_rates(
    reference_date: str | pd.Timestamp,
    settlement_date: str | pd.Timestamp,
) -> pd.DataFrame:
    """
    Fetch the NTN-B Anbima indicative rates and calculate the spot rates for the bonds.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.
        settlement_date (str | pd.Timestamp): The reference date for settlement.

    Returns:
        pd.DataFrame: DataFrame containing the maturity dates and corresponding real
            spot rates (RSR).
    """
    df_ytm = anbima_rates(reference_date)
    maturity_dates = df_ytm["MaturityDate"]
    ytm_rates = df_ytm["IndicativeRate"]
    return spot_rates(settlement_date, maturity_dates, ytm_rates)


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

    ffwd = Interpolator(
        method="flat_forward",
        known_bdays=df_nsr["BDaysToExp"],
        known_rates=df_nsr["NSR_DI"],
    )
    # Calculate Real Spot Rate (RSR)
    df = spot_rates(settlement_date, maturity_dates, ytm_rates)
    df = df.rename(columns={"RSR": "RSR"})
    df["BDays"] = bday.count(reference_date, df["MaturityDate"])
    df["NSR_DI"] = df["BDays"].apply(ffwd.interpolate)

    # Calculate Breakeven Inflation Rate (BIR)
    df["BIR_DI"] = ((df["NSR_DI"] + 1) / (df["RSR"] + 1)) - 1

    # Adjust BEI for DI spread in prefixed bonds
    ffwd = Interpolator(
        method="flat_forward",
        known_bdays=df_nsr["BDaysToExp"],
        known_rates=df_nsr["NSR_PRE"],
    )
    df["NSR_PRE"] = df["BDays"].apply(ffwd.interpolate)
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


def spot_rates0(
    settlement_date: str | pd.Timestamp,
    maturity_dates: pd.Series,
    ytm_rates: pd.Series,
) -> pd.DataFrame:
    """
    Calculate the spot rates for NTN-B bonds based on given settlement date, maturity
    dates, and YTM rates.

    Args:
        settlement_date (str | pd.Timestamp): The reference date for settlement.
        maturity_dates (pd.Series): Series of maturity dates for the bonds. ytm_rates
        (pd.Series): Series of Yield to Maturity rates corresponding to the
            maturity dates.

    Returns:
        pd.DataFrame: DataFrame containing the maturity dates and corresponding real
            spot rates (RSR).

    Notes:
        The calculation of the spot rates for NTN-B bonds considers the following steps:
            - Map all all possible payment dates up to the longest maturity date.
            - Interpolate the YTM rates in the intermediate payment dates.
            - Calculate the NTN-B quotation for each maturity date.
            - Calculate the real spot rates (RSR) for each maturity date.
    """
    # COUPON = (1.06) ** 0.5 - 1  # Coupon without rounding
    # Validate and normalize the settlement date
    settlement_date = dv.normalize_date(settlement_date)

    # Create the interpolator object
    flat_fwd = Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement_date, maturity_dates),
        known_rates=ytm_rates,
    )

    # Generate coupon dates up to the longest maturity date
    all_coupon_dates = coupon_dates_map(
        start=settlement_date,
        end=maturity_dates.max(),
    )

    # Create a DataFrame with all coupon dates
    df = pd.DataFrame(all_coupon_dates, columns=["MaturityDate"])

    # Add auxiliary columns for calculations
    df["BDays"] = bday.count(settlement_date, df["MaturityDate"])
    df["YTM"] = df["BDays"].apply(flat_fwd.interpolate)
    df["RSR"] = 0.0

    # Main loop to calculate spot rates
    for index in df.index:
        maturity_date = df.at[index, "MaturityDate"]
        # Get the coupon dates for the bond without the last one (principal + coupon)
        coupon_dates_wo_last = coupon_dates(settlement_date, maturity_date)[:-1]  # noqa

        # Create a temporary DataFrame as a subset of the main DataFrame
        dft = df.query("MaturityDate in @coupon_dates_wo_last").reset_index(drop=True)

        # Create the Series that will be used to calculate the discounted cash flows
        cash_flows = pd.Series(COUPON_PMT, index=dft.index)
        spot_rates = dft["RSR"]
        periods = dft["BDays"] / 252

        # Calculate the present value of the cash flows (discounted cash flows)
        discounted_cash_flows = cash_flows / (1 + spot_rates) ** periods

        # Calculate the real spot rate (RSR) for the bond using local variables
        bd = df.at[index, "BDays"]
        ytm = df.at[index, "YTM"]
        q = quotation(settlement_date, maturity_date, ytm) / 100
        dcf = discounted_cash_flows.sum()
        df.at[index, "RSR"] = ((COUPON_PMT + 1) / (q - dcf)) ** (252 / bd) - 1

    # Drop the BDays column, remove intermediate cupon dates and reset the index.
    return (
        df.drop(columns=["BDays"])
        .query("MaturityDate in @maturity_dates")
        .reset_index(drop=True)
    )
