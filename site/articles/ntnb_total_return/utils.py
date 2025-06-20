import pandas as pd
from pyield import ntnb, bday
import streamlit_functions.config as cfg


def get_coupon_payments(start_date, end_date, maturity_date):
    """
    Calculate coupon payments received between start_date and end_date

    Parameters:
    -----------
    start_date : pd.Timestamp
        The initial date for return calculation
    end_date : pd.Timestamp
        The final date for return calculation
    maturity_date : pd.Timestamp
        The maturity date of the NTN-B bond

    Returns:
    --------
    float
        The sum of all coupon payments received between start_date and end_date
    """
    # Get all cash flows for the bond
    cash_flows_df = ntnb.cash_flows(start_date, maturity_date)
    cash_flows_df["PaymentDate"] = pd.to_datetime(cash_flows_df["PaymentDate"]).dt.date

    # Filter cash flows that occur between start_date and end_date
    relevant_cash_flows = cash_flows_df[
        (cash_flows_df["PaymentDate"] > start_date)
        & (cash_flows_df["PaymentDate"] <= end_date)
    ].copy()
    # relevant_cash_flows['PaymentDate'] = pd.to_datetime(relevant_cash_flows['PaymentDate']).dt.date

    if relevant_cash_flows.empty:
        # PaymentDate  CashFlow  PaymentAmount
        return pd.DataFrame({
            "PaymentDate": [],
            "CashFlow": [],
            "PaymentAmount": [],
        })

    # Calculate the actual payment amounts using VNA
    payments = []
    for _, row in relevant_cash_flows.iterrows():
        payment_date = row["PaymentDate"]
        cash_flow_pct = row["CashFlow"]

        # Get the VNA for the payment date
        try:
            vna = cfg.df_vna_base.query("reference_date == @payment_date")[
                "vna"
            ].values[0]
        except IndexError:
            # If exact date not found, find the closest previous date
            closest_date = cfg.df_vna_base[
                cfg.df_vna_base["reference_date"] <= payment_date
            ]["reference_date"].max()
            vna = cfg.df_vna.query("reference_date == @closest_date")["vna_du"].values[
                0
            ]
            print(f"Using VNA from {closest_date} for payment date {payment_date}")
            print(f"Payment date: {payment_date}")

        # Calculate the actual payment amount
        payment_amount = vna * (cash_flow_pct / 100)
        payments.append(payment_amount)

    relevant_cash_flows["PaymentAmount"] = payments

    return relevant_cash_flows


def decompose_ntnb_return(start_date, end_date, maturity_date):
    """
    Decompose the return of an NTN-B bond between two dates, including coupon payments.

    Parameters:
    -----------
    start_date : pd.Timestamp
        The initial date for return calculation
    end_date : pd.Timestamp
        The final date for return calculation
    maturity_date : pd.Timestamp
        The maturity date of the NTN-B bond

    Returns:
    --------
    dict
        A dictionary containing the total return and its components
    """
    # Get coupon payments during the period
    coupon_payments = get_coupon_payments(start_date, end_date, maturity_date)

    if coupon_payments.empty:
        dates_calculation = [start_date]
        dates_calculation.append(end_date)
        dates_calculation.sort(reverse=True)

    else:
        coupon_payments["PaymentDate"] = pd.to_datetime(
            coupon_payments["PaymentDate"]
        ).dt.date

        dates_calculation = [start_date]
        dates_calculation.extend(coupon_payments["PaymentDate"].tolist())
        dates_calculation.append(end_date)
        dates_calculation.sort(reverse=True)

    inflation_return_total = []
    mark_to_market_total = []
    yield_return_total = []

    for i in range(len(dates_calculation) - 1):
        if i == 0:
            coupons_to_add = 0
        else:
            coupons_to_add = (1.06) ** (1 / 2) - 1

        start_date_coupons = dates_calculation[i + 1]
        start_date_coupons = bday.offset(start_date_coupons, 0).date()

        end_date_coupons = dates_calculation[i]
        end_date_coupons = bday.offset(end_date_coupons, 0).date()

        vna_start = cfg.df_vna.query("reference_date == @start_date_coupons")[
            "vna_du"
        ].values[0]
        vna_end = cfg.df_vna.query("reference_date == @end_date_coupons")[
            "vna_du"
        ].values[0]

        # Get indicative rates for both dates
        rate_start = cfg.df_ntnb.query(
            "ReferenceDate == @start_date_coupons and MaturityDate == @maturity_date"
        )["IndicativeRate"].values[0]

        rate_end = cfg.df_ntnb.query(
            "ReferenceDate == @end_date_coupons and MaturityDate == @maturity_date"
        )["IndicativeRate"].values[0]

        # Calculate quotations
        quotation_start = (
            ntnb.quotation(start_date_coupons, maturity_date, rate_start) / 100
        )
        quotation_end = (
            ntnb.quotation(end_date_coupons, maturity_date, rate_end) / 100
        ) + coupons_to_add
        quotation_hybrid = (
            ntnb.quotation(end_date_coupons, maturity_date, rate_start) / 100
        ) + coupons_to_add

        price_start = quotation_start * vna_start
        price_end = quotation_end * vna_end

        # Calculate total return
        total_return = (price_end / price_start) - 1

        # Calculate inflation component
        inflation_return = vna_end / vna_start

        # Calculate yield component
        mark_to_market = quotation_end / quotation_hybrid
        real_yield = quotation_hybrid / quotation_start

        cross_check = (mark_to_market * real_yield * inflation_return) - 1

        if cross_check - total_return > 0.0001:
            print("Cross check failed")
            print(f"Cross check: {cross_check}")
            print(f"Total return: {total_return}")

            return None

        inflation_return_total.append(inflation_return)
        mark_to_market_total.append(mark_to_market)
        yield_return_total.append(real_yield)

    return inflation_return_total, mark_to_market_total, yield_return_total
