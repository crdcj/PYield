import numpy as np
import pandas as pd

from . import bday
from . import date_validator as dv
from .fetchers.anbima import anbima

COUPON = 48.81
INTER_PMT = COUPON
FINAL_PMT = 1000.0 + INTER_PMT


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

    # Create a Series with the coupon dates
    payment_dates = pd.Series(coupon_dates(settlement_date, maturity_date))

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement_date, payment_dates)

    # Set the cash flow at maturity to 100, otherwise set it to the coupon
    cash_flows = np.where(payment_dates == maturity_date, FINAL_PMT, INTER_PMT)

    # Calculate the number of periods truncated to 14 decimal places
    num_periods = _truncate(bdays / 252, 14)

    discount_factor = (1 + discount_rate) ** num_periods

    # Calculate the present value of each cash flow (DCF) rounded to 10 decimal places
    discounted_cash_flows = (cash_flows / discount_factor).round(10)

    # Return the quotation (the dcf sum) truncated to 4 decimal places
    np_price = _truncate(discounted_cash_flows.sum(), 2)
    return float(np_price)


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


def indicative_rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
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
    return df[keep_columns].copy()
