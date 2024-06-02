import pandas as pd

from . import bday


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
        settlement_date (str | pd.Timestamp): Settlement date in 'YYYY-MM-DD' format.
        maturity_date (str | pd.Timestamp): Maturity date in 'YYYY-MM-DD' format.
        discount_rate (float): The yield to maturity (YTM) of the NTN-B, which is the
            discount rate used to calculate the present value of the cash flows.

    Returns:
        float: The NTN-B quotation truncated to 4 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - The semi-annual coupon is set to 2.956301, which represents a 6% annual
          coupon rate compounded semi-annually and rounded to 6 decimal places as per
          Anbima rules.

    Examples:
        >>> calculate_ntnb_quotation('2024-05-31', '2035-05-15', 0.061490)
        99.3651
        >>> calculate_ntnb_quotation('2024-05-31', '2060-08-15', 0.061878)
        99.5341
    """
    # Convert dates to pandas datetime format
    settlement_date = pd.to_datetime(settlement_date)
    maturity_date = pd.to_datetime(maturity_date)

    # Constants
    SEMIANNUAL_COUPON = 2.956301  # round(100 * ((0.06 + 1) ** 0.5 - 1), 6)

    # Initialize variables
    cash_flow_date = maturity_date
    quotation = 0.0

    # Iterate backwards from the maturity date to the settlement date
    while cash_flow_date > settlement_date:
        # Calculate the number of business days between settlement and cash flow dates
        num_of_bdays = bday.count_bdays(settlement_date, cash_flow_date)

        # Set the cash flow for the period
        cash_flow = SEMIANNUAL_COUPON
        if cash_flow_date == maturity_date:
            cash_flow += 100  # Adding principal repayment at maturity

        # Calculate the number of periods truncated to 14 decimal places
        annualized_period = truncate((num_of_bdays / 252), 14)

        # Calculate the present value of the cash flow (discounted cash flow)
        present_value = cash_flow / ((1 + discount_rate) ** annualized_period)

        # Add the present value for the period to the total quotation
        quotation += round(present_value, 10)

        # Move the cash flow date back 6 months
        cash_flow_date -= pd.DateOffset(months=6)

    # Return the quotation truncated to 4 decimal places
    return truncate(quotation, 4)
