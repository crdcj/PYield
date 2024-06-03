import pandas as pd

from . import bday
from . import date_validator as dv


def truncate(number: float, digits: int) -> float:
    """
    Truncate a number to a specified number of decimal places.

    Parameters:
        number (float): The number to be truncated.
        digits (int): The number of decimal places to keep.

    Returns:
        float: The truncated number.
    """
    stepper = 10.0**digits
    return int(number * stepper) / stepper


def calculate_ntnb_quotation(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
    discount_rate: float,
) -> float:
    """
    Calculate the NTN-B quotation using Anbima rules.

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
        >>> calculate_ntnb_quotation("2024-05-31", "2035-05-15", 0.061490)
        99.3651
        >>> calculate_ntnb_quotation("2024-05-31", "2060-08-15", 0.061878)
        99.5341
    """
    # Validate and normalize dates
    settlement_date = dv.normalize_date(settlement_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Constants
    SEMIANNUAL_COUPON = 2.956301  # round(100 * ((0.06 + 1) ** 0.5 - 1), 6)

    # Initialize variables
    coupon_date = maturity_date
    quotation = 0.0

    # Iterate backwards from the maturity date to the settlement date
    while coupon_date >= settlement_date:
        # Calculate the number of business days between settlement and cash flow dates
        num_of_bdays = bday.count_bdays(settlement_date, coupon_date)

        # Set the cash flow for the period
        cash_flow = SEMIANNUAL_COUPON
        if coupon_date == maturity_date:
            cash_flow += 100  # Adding principal repayment at maturity

        # Calculate the number of periods truncated to 14 decimal places
        annualized_period = truncate((num_of_bdays / 252), 14)

        # Calculate the present value of the cash flow (discounted cash flow)
        present_value = cash_flow / ((1 + discount_rate) ** annualized_period)

        # Add the present value for the period to the quotation
        quotation += round(present_value, 10)

        # Move the coupon date (cash flow date) back 6 months
        coupon_date -= pd.DateOffset(months=6)

    # Return the quotation truncated to 4 decimal places
    return truncate(quotation, 4)


def generate_all_coupon_dates(reference_date, last_coupon_date):
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


def generate_coupon_dates(
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


def calculate_ntnb_quotation_v2(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
    discount_rate: float,
) -> float:
    """
    Calculate the NTN-B quotation using Anbima rules.

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
        >>> calculate_ntnb_quotation("31-05-2024", "15-05-2035", 0.061490)
        99.3651
        >>> calculate_ntnb_quotation("31-05-2024", "15-08-2060", 0.061878)
        99.5341
    """
    # Validate and normalize dates
    settlement_date = dv.normalize_date(settlement_date)
    maturity_date = dv.normalize_date(maturity_date)

    # Constants
    SEMIANNUAL_COUPON = 2.956301  # round(100 * ((0.06 + 1) ** 0.5 - 1), 6)

    coupon_dates = generate_coupon_dates(settlement_date, maturity_date)

    # Create a dataframe with the coupon dates as a column
    df = pd.DataFrame(coupon_dates, columns=["CouponDate"])

    # Calculate the number of business days between settlement and cash flow dates
    df["BDays"] = bday.count_bdays(settlement_date, df["CouponDate"])

    df["CashFlow"] = SEMIANNUAL_COUPON

    # Adding principal repayment at maturity
    df.loc[df["CouponDate"] == maturity_date, "CashFlow"] += 100

    df["TotalPeriods"] = df["BDays"] / 252
    # Calculate the number of periods truncated to 14 decimal places
    df["TotalPeriods"] = df["TotalPeriods"].apply(lambda x: truncate(x, 14))

    # Calculate the present value of the cash flow (discounted cash flow)
    df["PresentValue"] = df["CashFlow"] / (1 + discount_rate) ** df["TotalPeriods"]

    # Round the present value to 10 decimal places
    df["PresentValue"] = df["PresentValue"].round(10)

    # Return the quotation truncated to 4 decimal places
    return df["PresentValue"].sum().round(4)
