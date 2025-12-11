import datetime as dt

import pandas as pd
import polars as pl

import pyield.converters as cv
import pyield.tn.tools as tl
from pyield import anbima, bday
from pyield.types import DateLike, has_nullable_args

"""
Constants calculated as per Anbima Rules and in base 100
Valid for NTN-C bonds with maturity date 01-01-2031:
PRINCIPAL = 100
COUPON_RATE = (0.12 + 1) ** 0.5 - 1  # 12% annual rate compounded semi-annually
COUPON_PMT = round(100 * COUPON_RATE, 6) -> 5.830052
FINAL_PMT = principal + last coupon payment = 100 + COUPON_PMT = 105.830052

All other NTN-C bonds will have different values for COUPON_PMT and FINAL_PMT:
COUPON_RATE = (0.06 + 1) ** 0.5 - 1  # 6% annual rate compounded semi-annually
COUPON_PMT = round(100 * COUPON_RATE, 6) -> 2.956301
FINAL_PMT = principal + last coupon payment = 100 + 2.956301

"""
COUPON_DAY = 1
COUPON_PMT_2031 = 5.830052
FINAL_PMT_2031 = 105.830052

COUPON_PMT = 2.956301
FINAL_PMT = 102.956301


def _get_coupon_pmt(maturity: dt.date) -> float:
    if maturity.year == 2031:  # noqa
        return COUPON_PMT_2031
    return COUPON_PMT


def _get_final_pmt(maturity: dt.date) -> float:
    if maturity.year == 2031:  # noqa
        return FINAL_PMT_2031
    return FINAL_PMT


def data(date: DateLike) -> pl.DataFrame:
    """
    Fetch the LTN Anbima indicative rates for the given reference date.

    Args:
        date (DateLike): The reference date for fetching the data.

    Returns:
        pl.DataFrame: DataFrame with columns "MaturityDate" and "IndicativeRate".

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.data("23-08-2024")
        shape: (1, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬──────────┬──────────┬────────────────┬─────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate  ┆ AskRate  ┆ IndicativeRate ┆ DIRate  │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---      ┆ ---      ┆ ---            ┆ ---     │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64      ┆ f64      ┆ f64            ┆ f64     │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪══════════╪══════════╪════════════════╪═════════╡
        │ 2024-08-23    ┆ NTN-C    ┆ 770100    ┆ 2000-07-01    ┆ … ┆ 0.061591 ┆ 0.057587 ┆ 0.059617       ┆ 0.11575 │
        └───────────────┴──────────┴───────────┴───────────────┴───┴──────────┴──────────┴────────────────┴─────────┘
    """  # noqa: E501
    return anbima.tpf_data(date, "NTN-C")


def payment_dates(
    settlement: DateLike,
    maturity: DateLike,
) -> pl.Series:
    """
    Generate all remaining coupon dates between a given date and the maturity date.
    The dates are inclusive. The NTN-C bond is determined by its maturity date.

    Args:
        settlement (DateLike): The settlement date (exlusive) to start generating
            the coupon dates.
        maturity (DateLike): The maturity date.

    Returns:
        pl.Series: Series of coupon dates within the specified range. Returns an
            empty Series if the maturity date is before the settlement date.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.payment_dates("21-03-2025", "01-01-2031")
        shape: (12,)
        Series: '' [date]
        [
            2025-07-01
            2026-01-01
            2026-07-01
            2027-01-01
            2027-07-01
            …
            2029-01-01
            2029-07-01
            2030-01-01
            2030-07-01
            2031-01-01
        ]
    """
    if has_nullable_args(settlement, maturity):
        return pl.Series(dtype=pl.Date)

    # Validate and normalize dates
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    # Check if maturity date is after the start date
    if maturity < settlement:
        return pl.Series(dtype=pl.Date)

    # Initialize loop variables
    coupon_date = maturity
    coupon_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while coupon_date > settlement:
        coupon_dates.append(coupon_date)
        # Move the coupon date back 6 months
        coupon_date -= pd.DateOffset(months=6)
        coupon_date = coupon_date.date()  # DateOffset returns a Timestamp

    return pl.Series(coupon_dates).sort()


def cash_flows(
    settlement: DateLike,
    maturity: DateLike,
) -> pl.DataFrame:
    """
    Generate the cash flows for NTN-C bonds between the settlement and maturity dates.

    Args:
        settlement (DateLike): The settlement date (exclusive) to start generating
            the cash flows.
        maturity (DateLike): The maturity date of the bond.

    Returns:
        pl.DataFrame: DataFrame with columns "PaymentDate" and "CashFlow".

    Returned columns:
        - PaymentDate: The payment date of the cash flow
        - CashFlow: Cash flow value for the bond

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.cash_flows("21-03-2025", "01-01-2031")
        shape: (12, 2)
        ┌─────────────┬────────────┐
        │ PaymentDate ┆ CashFlow   │
        │ ---         ┆ ---        │
        │ date        ┆ f64        │
        ╞═════════════╪════════════╡
        │ 2025-07-01  ┆ 5.830052   │
        │ 2026-01-01  ┆ 5.830052   │
        │ 2026-07-01  ┆ 5.830052   │
        │ 2027-01-01  ┆ 5.830052   │
        │ 2027-07-01  ┆ 5.830052   │
        │ …           ┆ …          │
        │ 2029-01-01  ┆ 5.830052   │
        │ 2029-07-01  ┆ 5.830052   │
        │ 2030-01-01  ┆ 5.830052   │
        │ 2030-07-01  ┆ 5.830052   │
        │ 2031-01-01  ┆ 105.830052 │
        └─────────────┴────────────┘
    """
    if has_nullable_args(settlement, maturity):
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    # Validate and normalize dates
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    # Get the coupon dates between the settlement and maturity dates
    pay_dates = payment_dates(settlement, maturity)

    # Return empty DataFrame if no payment dates (settlement >= maturity)
    if pay_dates.is_empty():
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    # Get the right coupon payment and final payment values
    coupon_pmt = _get_coupon_pmt(maturity)
    final_pmt = _get_final_pmt(maturity)

    # Build dataframe and assign cash flows using Polars expression (avoid NumPy)
    df = pl.DataFrame({"PaymentDate": pay_dates}).with_columns(
        pl.when(pl.col("PaymentDate") == maturity)
        .then(final_pmt)
        .otherwise(coupon_pmt)
        .alias("CashFlow")
    )
    return df


def quotation(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calculate the NTN-C quotation in base 100 using Anbima rules.

    Args:
        settlement (DateLike): The settlement date of the operation.
        maturity (DateLike): The maturity date of the NTN-C bond.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-C.

    Returns:
        float: The NTN-C quotation truncated to 4 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - The semi-annual coupon is set to 2.956301, which represents a 6% annual
          coupon rate compounded semi-annually and rounded to 6 decimal places as per
          Anbima rules.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.quotation("21-03-2025", "01-01-2031", 0.067626)
        126.4958
    """
    if has_nullable_args(settlement, maturity, rate):
        return float("nan")

    cf_df = cash_flows(settlement, maturity)
    if cf_df.is_empty():
        return float("nan")

    cf_dates = cf_df["PaymentDate"]
    cf_values = cf_df["CashFlow"]

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement, cf_dates)

    # Calculate the number of periods truncated as per Anbima rules
    num_of_years = tl.truncate(bdays / 252, 14)

    discount_factor = (1 + rate) ** num_of_years

    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    cf_present_value = (cf_values / discount_factor).round(10)

    # Return the quotation (the dcf sum) truncated as per Anbima rules
    return tl.truncate(cf_present_value.sum(), 4)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calculate the NTN-C price using Anbima rules.

    price = VNA * quotation / 100

    Args:
        vna (float): The nominal value of the NTN-C bond.
        quotation (float): The NTN-C quotation in base 100.

    Returns:
        float: The NTN-C price truncated to 6 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.price(6598.913723, 126.4958)
        8347.348705
    """
    if has_nullable_args(vna, quotation):
        return float("nan")
    return tl.truncate(vna * quotation / 100, 6)


def duration(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calculate the Macaulay duration of the NTN-C bond in business years.

    Args:
        settlement (DateLike): The settlement date of the operation.
        maturity (DateLike): The maturity date of the NTN-C bond.
        rate (float): The discount rate used to calculate the duration.

    Returns:
        float: The Macaulay duration of the NTN-C bond in business years.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.duration("21-03-2025", "01-01-2031", 0.067626)
        4.405363320448
    """
    if has_nullable_args(settlement, maturity, rate):
        return float("nan")

    df = cash_flows(settlement, maturity)
    if df.is_empty():
        return float("nan")

    b_years = bday.count(settlement, df["PaymentDate"]) / 252
    dcf = df["CashFlow"] / (1 + rate) ** b_years
    duration = (dcf * b_years).sum() / dcf.sum()
    # Truncar para 14 casas decimais para repetibilidade dos resultados
    return tl.truncate(duration, 14)
