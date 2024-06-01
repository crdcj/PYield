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

SEMIANNUAL_COUPON = 0.02956301  # round((0.06 + 1) ** 0.5 - 1, 8)


def calculate_ntnb_quotation(settlement_date, maturity_date, discount_rate):
    # Convert dates to pandas datetime format
    settlement_date = pd.to_datetime(settlement_date)
    maturity_date = pd.to_datetime(maturity_date)

    # If the bond has already matured, the quotation is zero
    if settlement_date > maturity_date:
        return 0.0

    # Initialize variables
    cash_flow_date = maturity_date
    ntnb_quotation = 0.0

    # Iterate backwards from the maturity date to the settlement date
    while cash_flow_date > settlement_date:
        # Calculate the number of business days between settlement and cash flow dates
        business_days_count = bday.count_bdays(settlement_date, cash_flow_date)

        # Set the cash flow for the period
        cash_flow = SEMIANNUAL_COUPON
        if cash_flow_date == maturity_date:
            cash_flow += 1  # Adding principal repayment at maturity

        # Calculate the discount factor for the period
        discount_factor = (1 + discount_rate) ** (business_days_count / 252)

        # Calculate the present value of the cash flow
        present_value = round(cash_flow / discount_factor, 10)

        # Accumulate the present value into the NTN-B quotation
        ntnb_quotation += present_value

        # Move to the previous cash flow date (6 months earlier)
        cash_flow_date -= pd.DateOffset(months=6)

    # Return the final NTNB quotation rounded to 6 decimal places
    return round(ntnb_quotation, 6)
