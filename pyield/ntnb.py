"""
Rounding, Truncation, Information Provision, and Number of Decimal Places for
Calculations of Public Bonds
------------------------------------------------------------
                               LTN   NTN-F NTN-B NTN-C LFT
------------------------------------------------------------
Return Rate (% p.a.)           T4/I4 T4/I4 T4/I4 T4/I4 T4/I4
Semi-Annual Interest (%)       -     R5    R6    R6    -
Discounted Cash Flow           -     R9    R10   R10   -
Quotation                      -     -     T4    T4    T4
VNA                            -     -     T6/I6 T6/I6 T6/I6
VNA Projections                -     -     T6    T6    T6
Accumulated SELIC Rate Factor  -     -     -     -     R16
Projections                    -     -     R2    R2    -
Pro Rata Factor (Projections)  -     -     T14   T14   -
Official Month Variation       -     -     T16   T16   -
Days Exponential               T14   T14   T14   T14   T14
Unit Price (PU)                T6/I6 T6/I6 T6    T6    T6
Financial Value (R$)           T2    T2    T2    T2    T2

Notes:
------
T = Truncated; R = Rounded; I = Informed
VNA = Updated Nominal Value
"""

import pandas as pd

from . import bday

SEMIANNUAL_COUPON = 2.956301  # round(100 * (0.06 + 1) ** 0.5 - 1, 6)


def truncate(number, digits):
    stepper = 10.0**digits
    return int(number * stepper) / stepper


def calculate_ntnb_quotation(settlement_date, maturity_date, discount_rate):
    # Convert dates to pandas datetime format
    settlement_date = pd.to_datetime(settlement_date)
    maturity_date = pd.to_datetime(maturity_date)

    # If the bond has already matured, the quotation is zero
    if settlement_date > maturity_date:
        return 0.0

    # Initialize variables
    cash_flow_date = maturity_date
    present_values = []

    # Iterate backwards from the maturity date to the settlement date
    while cash_flow_date > settlement_date:
        # Calculate the number of business days between settlement and cash flow dates
        business_days_count = bday.count_bdays(settlement_date, cash_flow_date)

        # Set the cash flow for the period
        cash_flow = SEMIANNUAL_COUPON
        if cash_flow_date == maturity_date:
            cash_flow += 100  # Adding principal repayment at maturity

        # Calculate the exponential factor
        exp_factor = truncate((business_days_count / 252), 14)

        # Calculate the present value of the cash flow
        present_value = cash_flow / ((1 + discount_rate) ** exp_factor)

        # Store the present value rounded to 10 decimal places
        present_values.append(round(present_value, 10))

        # Move to the previous cash flow date (6 months earlier)
        cash_flow_date -= pd.DateOffset(months=6)

    # Return the final NTN-B quotation rounded to 4 decimal places
    return truncate(sum(present_values), 4)
