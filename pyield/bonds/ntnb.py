import numpy as np
import pandas as pd

from .. import bday
from .. import date_converter as dc
from .. import interpolator as it
from ..data import anbima
from . import bond_tools as bt

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


def _is_maturity_valid(maturity: pd.Timestamp) -> bool:
    """
    Check if the maturity date is a valid NTN-B maturity date.

    Args:
        maturity (pd.Timestamp): The maturity date to be checked.

    Returns:
        bool: True if the maturity date is valid, False otherwise.
    """
    return maturity.day == COUPON_DAY and maturity.month in COUPON_MONTHS


def _check_maturities(
    maturities: pd.Timestamp | list[pd.Timestamp] | pd.Series,
) -> None:
    """
    Check if the maturity dates are valid NTN-B maturities.

    Args:
        maturities (pd.Timestamp | list[pd.Timestamp] | pd.Series): The maturity
            date(s) to be checked.

    Raises:
        ValueError: If the maturity dates are not valid NTN-B maturities.
    """
    if isinstance(maturities, pd.Timestamp):
        maturities = [maturities]
    checked_maturities = [_is_maturity_valid(maturity) for maturity in maturities]
    if not all(checked_maturities):
        raise ValueError("NTN-B maturity must be 15/02, 15/05, 15/08 or 15/11.")


def rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the bond indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame containing the maturity dates and indicative rates
            for the NTN-B bonds.
    """
    ntnb_rates = anbima.rates(reference_date, "NTN-B")
    if ntnb_rates.empty:
        return pd.DataFrame()
    return ntnb_rates[["MaturityDate", "IndicativeRate"]]


def maturities(reference_date: str | pd.Timestamp) -> pd.Series:
    """
    Get the bond maturities available for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.Series: Series containing the maturity dates for the NTN-B bonds.
    """
    df_rates = rates(reference_date)
    return df_rates["MaturityDate"]


def _generate_coupon_dates_map(
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
    coupon_dates = pd.date_range(start=first_coupon_date, end=end, freq="3MS")

    # Offset dates by 14 in order to have day 15 of the month
    coupon_dates += pd.Timedelta(days=14)

    # First coupon date must be after the reference date, otherwise, it can lead to
    # division by zero where BDays == 0 (bootstrap method for instance)
    coupon_dates = coupon_dates[coupon_dates > start]

    return pd.Series(coupon_dates).reset_index(drop=True)


def payment_dates(
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
) -> pd.Series:
    """
    Generate all remaining coupon dates between a given date and the maturity date.
    The dates are inclusive. Coupon payments are made on the 15th of February, May,
    August, and November (15-02, 15-05, 15-08, and 15-11 of each year). The NTN-B
    bond is determined by its maturity date.

    Args:
        settlement (str | pd.Timestamp): The settlement date (exlusive) to
            start generating coupon dates.
        maturity (str | pd.Timestamp): The maturity date.

    Returns:
        pd.Series: Series of coupon dates within the specified range.
    """
    # Validate and normalize dates
    settlement = dc.convert_date(settlement)
    maturity = dc.convert_date(maturity)
    _check_maturities(maturity)

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
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
) -> pd.DataFrame:
    """
    Generate the cash flows for NTN-B bonds between the settlement and maturity dates.

    Args:
        settlement (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.

    Returns:
        pd.DataFrame: DataFrame containing the cash flows for the NTN-B bond.

    Returned columns:
        - PaymentDate: The payment date of the cash flow
        - CashFlow: Cash flow value for the bond
    """
    # Validate and normalize dates
    settlement = dc.convert_date(settlement)
    maturity = dc.convert_date(maturity)

    # Get the coupon dates between the settlement and maturity dates
    p_dates = payment_dates(settlement, maturity)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    cfs = np.where(p_dates == maturity, FINAL_PMT, COUPON_PMT).tolist()

    # Return a dataframe with the payment dates and cash flows
    return pd.DataFrame(data={"PaymentDate": p_dates, "CashFlow": cfs})


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
    _check_maturities(maturity)

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
    return bt.truncate(vna * quotation / 100, 6)


def spot_rates(
    settlement: str | pd.Timestamp,
    maturities: pd.Series,
    rates: pd.Series,
) -> pd.DataFrame:
    """
    Calculate the spot rates for NTN-B bonds using the bootstrap method.

    The bootstrap method is a process used to determine spot rates from
    the yields of a series of bonds. It involves iteratively solving for
    the spot rates that discount each bond's cash flows to its current
    price.


    Args:
        settlement (str | pd.Timestamp): The reference date for settlement.
        maturities (pd.Series): Series of maturity dates for the bonds.
        rates (pd.Series): Series of yield to maturity rates.

    Returns:
        pd.DataFrame: DataFrame containing the maturity dates and corresponding real
            spot rates.

    Notes:
        The calculation of the spot rates for NTN-B bonds considers the following steps:
            - Map all all possible payment dates up to the longest maturity date.
            - Interpolate the YTM rates in the intermediate payment dates.
            - Calculate the NTN-B quotation for each maturity date.
            - Calculate the real spot rates for each maturity date.
            - Columns in the returned DataFrame:
                - MaturityDate: The maturity date of the bond.
                - YTM: The yield to maturity rate for the bond.
                - SpotRate: The real spot rate for the bond.
    """
    # Process and validate the input data
    settlement = dc.convert_date(settlement)
    maturities = pd.to_datetime(maturities, errors="raise", dayfirst=True)
    _check_maturities(maturities)

    # Create the interpolator object
    ff_interpolator = it.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, maturities),
        known_rates=rates,
    )

    # Generate coupon dates up to the longest maturity date
    all_coupon_dates = _generate_coupon_dates_map(
        start=settlement, end=maturities.max()
    )

    # Create a DataFrame with all coupon dates and the corresponding YTM
    df = pd.DataFrame(data=all_coupon_dates, columns=["MaturityDate"])
    df["BDays"] = bday.count(settlement, df["MaturityDate"])
    df["BYears"] = df["BDays"] / 252
    df["YTM"] = df["BDays"].apply(ff_interpolator)
    df["Coupon"] = COUPON_PMT
    df["SpotRate"] = np.nan

    # The Bootstrap loop to calculate spot rates
    for index, row in df.iterrows():
        # Get the cash flow dates for the bond
        cf_dates = payment_dates(settlement, row["MaturityDate"])

        # If there is only one coupon date and this date is the first maturity date
        # of an existing bond, the ytm rate is also a spot rate.
        if len(cf_dates) == 1 and cf_dates[0] == maturities[0]:
            df.at[index, "SpotRate"] = row["YTM"]
            continue

        # Calculate the present value of the cash flows without last payment
        cf_dates = cf_dates[:-1]
        cf_df = df.query("MaturityDate in @cf_dates").reset_index(drop=True)
        cf_present_value = bt.calculate_present_value(
            cash_flows=cf_df["Coupon"],
            rates=cf_df["SpotRate"],
            periods=cf_df["BYears"],
        )

        # Calculate the Spot Rate for the bond
        bond_price = quotation(settlement, row["MaturityDate"], row["YTM"])
        price_factor = FINAL_PMT / (bond_price - cf_present_value)
        df.at[index, "SpotRate"] = price_factor ** (1 / row["BYears"]) - 1

    df.drop(columns=["BDays", "BYears", "Coupon"], inplace=True)
    # Force Float64 type in float columns to standardize the output
    df = df.astype({"YTM": "Float64", "SpotRate": "Float64"})
    # Return the result without the intermediate coupon dates (virtual bonds)
    return df.query("MaturityDate in @maturities").reset_index(drop=True)


def bei_rates(
    settlement: str | pd.Timestamp,
    real_maturities: pd.Series,
    real_rates: pd.Series,
    nominal_maturities: pd.Series,
    nominal_rates: pd.Series,
) -> pd.DataFrame:
    """
    Calculate the Breakeven Inflation (BEI) for NTN-B bonds based on nominal and real
    interest rates.

    Args:
        settlement (str or pd.Timestamp): The settlement date for the bonds.
        real_maturities (pd.Series): Series of maturity dates for the inflation-indexed
            bonds (NTN-B).
        real_rates (pd.Series): Series of real interest rates (Yield to Maturity - YTM)
            corresponding to the inflation-indexed bonds' maturity dates.
        nominal_maturities (pd.Series): Series of maturity dates for the nominal bonds
            (e.g., DI or prefixed bonds).
        nominal_rates (pd.Series): Series of nominal interest rates corresponding to the
            nominal maturity dates.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated breakeven inflation rates.

    Returned columns:
        - MaturityDate: The maturity date of the bonds.
        - BDays: Number of business days from the settlement date to the maturity date.
        - RIR: The calculated Real Interest Rate based on the spot rates.
        - NIR: Nominal Interest Rate interpolated for the corresponding maturity date.
        - BEI: The calculated Breakeven Inflation Rate, which represents the inflation
            rate that equalizes the real and nominal yields.

    Notes:
        The BEI is calculated by comparing the nominal and real interest rates,
        indicating the market's inflation expectations over the period from the
        settlement date to the bond's maturity.
    """
    # Normalize input dates
    settlement = dc.convert_date(settlement)
    real_maturities = pd.to_datetime(real_maturities, errors="coerce", dayfirst=True)

    # Calculate Real Interest Rate (RIR)
    df = spot_rates(settlement, real_maturities, real_rates)
    df["BDays"] = bday.count(settlement, df["MaturityDate"])
    df = df.rename(columns={"SpotRate": "RIR"})

    nir_interplator = it.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, nominal_maturities),
        known_rates=nominal_rates,
    )

    df["NIR"] = df["BDays"].apply(nir_interplator).astype("Float64")
    # Calculate Breakeven Inflation Rate (BEI)
    df["BEI"] = ((df["NIR"] + 1) / (df["RIR"] + 1)) - 1

    cols_reordered = [
        "MaturityDate",
        "YTM",
        "RIR",
        "NIR",
        "BEI",
    ]
    return df[cols_reordered].copy()
