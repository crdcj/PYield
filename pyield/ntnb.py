import numpy as np
import pandas as pd

from . import bday
from . import date_validator as dv


def truncate(value, decimal_places):
    """
    Truncate a float or a Pandas Series to the specified decimal place.

    Parameters:
    value (float, pandas.Series): The value(s) to be truncated.
    decimal_places (int): The number of decimal places to truncate to.

    Returns:
    float or pandas.Series: The truncated value(s).
    """
    factor = 10**decimal_places
    return np.trunc(value * factor) / factor


def generate_all_payment_dates(reference_date, last_coupon_date):
    # Validate and normalize dates
    reference_date = dv.normalize_date(reference_date)
    last_coupon_date = dv.normalize_date(last_coupon_date)

    # Initialize the first coupon date based on the reference date
    reference_year = reference_date.year
    first_coupon_date = pd.Timestamp(f"{reference_year}-02-01")

    # Generate coupon dates
    dates = pd.date_range(start=first_coupon_date, end=last_coupon_date, freq="3MS")

    # Offset dates by 14 in order to have day 15 of the month
    dates = dates + pd.Timedelta(days=14)

    # First coupon date must be after the reference date
    return dates[dates > reference_date]


def generate_payment_dates(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
) -> pd.Series:
    # Validate and normalize dates
    settlement_date = dv.normalize_date(settlement_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Initialize loop variables
    coupon_date = maturity_date
    coupon_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while coupon_date >= settlement_date:
        coupon_dates.append(coupon_date)
        # Move the coupon date back 6 months
        coupon_date -= pd.DateOffset(months=6)

    # Return the coupon dates sorted in ascending order
    return pd.Series(coupon_dates).sort_values(ignore_index=True)


def calculate_quotation(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
    discount_rate: float,
) -> float:
    """
    Calculate the NTN-B quotation in base 100 using Anbima rules.

    Parameters:
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
        >>> calculate_quotation("31-05-2024", "15-05-2035", 0.061490)
        99.3651
        >>> calculate_quotation("31-05-2024", "15-08-2060", 0.061878)
        99.5341
    """
    # Semi-annual coupon value in base 100 and rounded to 6 decimal places
    COUPON = 2.956301  # round(100 * ((0.06 + 1) ** 0.5 - 1), 6)

    # Validate and normalize dates
    settlement_date = dv.normalize_date(settlement_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Create a Series with the coupon dates
    payment_dates = pd.Series(generate_payment_dates(settlement_date, maturity_date))

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count_bdays(settlement_date, payment_dates)

    # Set the cash flow at maturity to 100, otherwise set it to the coupon
    cf = np.where(payment_dates == maturity_date, COUPON + 100, COUPON)

    # Calculate the number of periods truncated to 14 decimal places
    n = truncate(bdays / 252, 14)

    # Calculate the present value of each cash flow (discounted cash flow)
    dcf = (cf / (1 + discount_rate) ** n).round(10)

    # Return the quotation (the dcf sum) truncated to 4 decimal places
    return truncate(dcf.sum(), 4)
