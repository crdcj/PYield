import numpy as np
import pandas as pd

from .. import bday
from .. import date_validator as dv
from ..fetchers.anbima import anbima
from ..interpolator import Interpolator
from . import utils as bu

# Constants
FACE_VALUE = 1000
COUPON_DAY = 1
COUPON_MONTHS = [1, 7]
COUPON_RATE = (0.10 + 1) ** 0.5 - 1  # 10% annual rate compounded semi-annually
COUPON_PMT = round(FACE_VALUE * COUPON_RATE, 5)  # Rounded as per Anbima rules
FINAL_PMT = FACE_VALUE + COUPON_PMT


def coupon_dates(
    start_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
) -> pd.Series:
    """
    Generate all remaining coupon dates between a given date and the maturity date.
    The dates are inclusive. Coupon payments are made on the 1st of January and July.
    The NTN-F bond is determined by its maturity date.

    Args:
        start_date (str | pd.Timestamp): The date to start generating coupon dates.
        maturity_date (str | pd.Timestamp): The maturity date.

    Returns:
        pd.Series: Series of coupon dates within the specified range.
    """
    # Validate and normalize dates
    start_date = dv.normalize_date(start_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Check if maturity date is after the start date
    if maturity_date < start_date:
        raise ValueError("Maturity date must be after the start date.")

    # Check if the maturity date is a valid NTN-F maturity date
    if maturity_date.day != COUPON_DAY or maturity_date.month not in COUPON_MONTHS:
        raise ValueError("NTN-F maturity date must be the 1st of January or July.")

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


def price(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
    discount_rate: float,
) -> float:
    """
    Calculate the NTN-F price using Anbima rules.

    Args:
        settlement_date (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity_date (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        discount_rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-F.

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

    # Validate and normalize dates
    settlement_date = dv.normalize_date(settlement_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Create a Series with the coupon dates
    payment_dates = pd.Series(coupon_dates(settlement_date, maturity_date))

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement_date, payment_dates)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    cash_flows = np.where(payment_dates == maturity_date, FINAL_PMT, COUPON_PMT)

    # Calculate the number of periods truncated as per Anbima rules
    num_periods = bu.truncate(bdays / 252, 14)

    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    discount_factor = (1 + discount_rate) ** num_periods
    discounted_cash_flows = (cash_flows / discount_factor).round(9)

    # Return the sum of the discounted cash flows truncated as per Anbima rules
    return bu.truncate(discounted_cash_flows.sum(), 6)


def coupon_dates_map(
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    adjust_for_bdays: bool = False,
) -> pd.Series:
    """
    Generate a map of all possible coupon dates between the start and end dates.
    The dates are inclusive. Coupon payments are made on the 1st of January and July.

    Args:
        start (str | pd.Timestamp): The start date.
        end (str | pd.Timestamp): The end date.
        adjust_for_bdays (bool, optional): If True, the coupon dates will be adjusted
            for business days. Defaults to False.

    Returns:
        pd.Series: Series of coupon dates within the specified range.
    """
    # Validate and normalize dates
    start = dv.normalize_date(start)
    end = dv.normalize_date(end)

    # Initialize the first coupon date based on the reference date
    reference_year = start.year
    first_coupon_date = pd.Timestamp(f"{reference_year}-01-01")

    # Generate coupon dates
    dates = pd.date_range(start=first_coupon_date, end=end, freq="6MS")

    # First coupon date must be after the reference date
    dates = dates[dates >= start]

    # Convert to Series and adjust for business days if necessary
    dates = pd.Series(dates).reset_index(drop=True)
    if adjust_for_bdays:
        dates = bday.offset(dates, 0)

    return dates


def anbima_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-F Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return anbima(bond_type="NTN-F", reference_date=reference_date)


def anbima_rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-F Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the maturity dates and corresponding rates.
    """
    df = anbima_data(reference_date)
    # Keep only the relevant columns for the output
    keep_columns = ["ReferenceDate", "BondType", "MaturityDate", "IndicativeRate"]
    # Promote MaturityDate to index
    return df[keep_columns].set_index("MaturityDate")


def spot_rates(
    settlement_date: str | pd.Timestamp,
    ltn_rates: pd.Series,
    ntnf_rates: pd.Series,
) -> pd.DataFrame:
    """
    Fetch NTN-F Anbima spot rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.
        ltn_rates (pd.Series): The LTN known rates, indexed by maturity date.
        ntnf_rates (pd.Series): The NTN-F known rates, indexed by maturity date.

    Returns:
        pd.DataFrame: A DataFrame containing the maturity dates and corresponding rates.
    """
    ltn_rate_interp = Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement_date, ltn_rates.index),
        known_rates=ltn_rates,
    )
    ntnf_rate_interp = Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement_date, ntnf_rates.index),
        known_rates=ntnf_rates,
    )
    last_ltn = ltn_rates.index.max()
    last_ntnf = ntnf_rates.index.max()
    all_coupon_dates = coupon_dates_map(settlement_date, last_ntnf)

    df = pd.DataFrame(data=all_coupon_dates, columns=["MaturityDate"])
    df["BDays"] = bday.count(start=settlement_date, end=df["MaturityDate"])
    df["YTM"] = df["BDays"].apply(ntnf_rate_interp)
    # Promote MaturityDate to index
    df = df.set_index("MaturityDate")
    df["SpotRate"] = 0.0

    for maturity in df.index:
        bdays = df.at[maturity, "BDays"]
        ytm = df.at[maturity, "YTM"]
        if maturity <= last_ltn:
            df.loc[maturity, "SpotRate"] = ltn_rate_interp(bdays)
        else:
            cp_dates_wo_last = coupon_dates(settlement_date, maturity)[:-1]  # noqa

            # Create a subset DataFrame without final payment
            df_inter = df.query("MaturityDate in @cp_dates_wo_last").reset_index(
                drop=True
            )

            # Calculate the present value of the cash flows (discounted cash flows)
            pv = bu.calculate_present_value(
                cash_flows=pd.Series(COUPON_PMT, index=df_inter.index),
                discount_rates=df_inter["SpotRate"],
                time_periods=df_inter["BDays"] / 252,
            )

            # Calculate the real spot rate (RSR) for the bond
            bond_price = price(settlement_date, maturity, ytm)
            spot_rate = ((FINAL_PMT) / (bond_price - pv)) ** (252 / bdays) - 1
            df.at[maturity, "SpotRate"] = spot_rate
