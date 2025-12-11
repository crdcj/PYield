from enum import Enum

import pandas as pd
import polars as pl

import pyield.converters as cv
import pyield.tn.tools as bt
from pyield import bday
from pyield.types import DateLike, has_nullable_args

"""
Global parameters for NTN-B1 bonds calculations.
These parameters are used to define the cash flows and payment structure of the bonds,
according to Commercial Name.
The parameters are initialized based on the commercial name of the NTN-B1 bond.

"""
AMORTIZATION_PAYMENT = 0
FINAL_AMORTIZATION_PAYMENT = 0
NUMBER_OF_AMORTIZATIONS = 0


class CommercialName(Enum):
    """
    Commercial Name Enum used to identify the kind of used NTN-B1 in the scope of the
    calculation (Renda+ or Educa+).
    """

    RENDA_MAIS = "Renda+"
    EDUCA_MAIS = "Educa+"


# Mapeamento estático do número de meses
# Isso é uma constante real (não muda durante a execução)
PARAMS_MAP = {
    CommercialName.RENDA_MAIS: 240,
    CommercialName.EDUCA_MAIS: 60,
}


def _get_bond_parameters(commercial_name: CommercialName) -> tuple[float, float, int]:
    """
    Returns amortization parameters based on commercial name.
    Returns: (amortization_payment, final_amortization_payment, number_of_amortizations)
    """
    try:
        n_amortizations = PARAMS_MAP[commercial_name]
    except KeyError:
        raise ValueError(f"Invalid commercial name: {commercial_name}")

    amortization_payment = 1 / n_amortizations
    final_amortization_payment = 1 - (amortization_payment * (n_amortizations - 1))

    return amortization_payment, final_amortization_payment, n_amortizations


def payment_dates(
    settlement: DateLike, maturity: DateLike, commercial_name: CommercialName
) -> pl.Series:
    """
    Generate all remaining amortization dates between a given date and the maturity
    date.
    The dates are inclusive. Payments are made from January 15th of the year of
    conversion to December 15 of maturity year.

    Args:
        settlement (DateLike): The settlement date (exclusive) to start generating
            the amortization dates.
        maturity (DateLike): The maturity date.
        commercial_name (CommercialName): The commercial name of the NTN-B1 bond
            (Renda+ or Educa+).

    Returns:
        pl.Series: Series of coupon dates within the specified range.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.payment_dates("10-05-2024", "15-12-2050", r_mais)
        shape: (240,)
        Series: 'payment_dates' [date]
        [
            2031-01-15
            2031-02-15
            2031-03-15
            2031-04-15
            2031-05-15
            …
            2050-08-15
            2050-09-15
            2050-10-15
            2050-11-15
            2050-12-15
        ]
    """
    if has_nullable_args(settlement, maturity, commercial_name):
        return pl.Series("payment_dates", dtype=pl.Date)

    # Validate and normalize dates
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    if maturity <= settlement:
        raise ValueError("Maturity date must be after the settlement date.")

    maturity = maturity.replace(day=15)

    # Get bond parameters
    _, _, n_amortizations = _get_bond_parameters(commercial_name)

    amtz_dates = [maturity - pd.DateOffset(months=i) for i in range(n_amortizations)]

    if len(amtz_dates) == 0:
        raise ValueError("No amortization dates found after settlement date.")

    pmt_dates = pl.Series(name="payment_dates", values=amtz_dates).cast(pl.Date)

    return pmt_dates.filter(pmt_dates > settlement).sort()


def cash_flows(
    settlement: DateLike, maturity: DateLike, commercial_name: CommercialName
) -> pl.DataFrame:
    """
    Generate the cash flows for NTN-B1 bonds between the settlement and maturity dates.

    Args:
        settlement (DateScalar): The settlement date (exclusive) to start generating
            the cash flows.
        maturity (DateScalar): The maturity date of the bond.
        commercial_name (CommercialName): The commercial name of the NTN-B1 bond
            (Renda+ or Educa+).

    Returns:
        pd.DataFrame: DataFrame with columns "PaymentDate" and "CashFlow".

    Returned columns:
        - PaymentDate: The payment date of the cash flow
        - CashFlow: Cash flow value for the bond

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.cash_flows("10-05-2024", "15-12-2060", r_mais)
        shape: (240, 2)
        ┌─────────────┬──────────┐
        │ PaymentDate ┆ CashFlow │
        │ ---         ┆ ---      │
        │ date        ┆ f64      │
        ╞═════════════╪══════════╡
        │ 2041-01-15  ┆ 0.004167 │
        │ 2041-02-15  ┆ 0.004167 │
        │ 2041-03-15  ┆ 0.004167 │
        │ 2041-04-15  ┆ 0.004167 │
        │ 2041-05-15  ┆ 0.004167 │
        │ …           ┆ …        │
        │ 2060-08-15  ┆ 0.004167 │
        │ 2060-09-15  ┆ 0.004167 │
        │ 2060-10-15  ┆ 0.004167 │
        │ 2060-11-15  ┆ 0.004167 │
        │ 2060-12-15  ┆ 0.004167 │
        └─────────────┴──────────┘

    """
    if has_nullable_args(settlement, maturity, commercial_name):
        return pl.DataFrame({"PaymentDate": [], "CashFlow": []})

    # Validate and normalize dates
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    # Get the coupon dates between the settlement and maturity dates
    p_dates = payment_dates(settlement, maturity, commercial_name)
    df = pl.DataFrame({"PaymentDate": p_dates})

    # Get bond parameters
    amort_payment, final_amort_payment, _ = _get_bond_parameters(commercial_name)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    df = df.with_columns(
        pl.when(pl.col("PaymentDate") == maturity)
        .then(final_amort_payment)
        .otherwise(amort_payment)
        .alias("CashFlow")
    )

    # Return a dataframe with the payment dates and cash flows
    return df


def quotation(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    commercial_name: CommercialName,
) -> float:
    """
    Calculate the NTN-B quotation in base 100 using Anbima rules.

    Args:
        settlement (DateScalar): The settlement date of the operation.
        maturity (DateScalar): The maturity date of the NTN-B bond.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-B.
        commercial_name (CommercialName): The commercial name of the NTN-B1 bond
            (Renda+ or Educa+).

    Returns:
        float: The NTN-B quotation truncated to 6 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.quotation("18-06-2025", "15-12-2084", 0.07010, r_mais)
        0.038332
    """
    if has_nullable_args(settlement, maturity, rate, commercial_name):
        return float("nan")

    cf_df = cash_flows(settlement, maturity, commercial_name)
    cf_dates = cf_df["PaymentDate"]
    cf_values = cf_df["CashFlow"]

    # Calculate the number of business days between settlement and cash flow dates
    b_days = bday.count(settlement, cf_dates)

    # Calculate the number of periods truncated as per Anbima rules
    num_of_years = bt.truncate(b_days / 252, 14)

    discount_factor = (1 + rate) ** num_of_years

    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    cf_present_value = (cf_values / discount_factor).round(10)

    # Return the quotation (the dcf sum) truncated as per Anbima rules
    return bt.truncate(cf_present_value.sum(), 6)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calculate the NTN-B1 price using Brazilian Treasury rules.

    Args:
        vna (float): The nominal value of the NTN-B bond.
        quotation (float): The NTN-B quotation in base 100.

    Returns:
        float: The NTN-B1 price truncated to 6 decimal places.

    References:
         - SEI Proccess 17944.005214/2024-09

    Examples:
        >>> from pyield import ntnb1
        >>> ntnb1.price(4299.160173, 99.3651 / 100)
        4271.864805
        >>> ntnb1.price(4315.498383, 100.6409 / 100)
        4343.156412
    """
    if has_nullable_args(vna, quotation):
        return float("nan")
    return bt.truncate(vna * quotation, 6)


def duration(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    commercial_name: CommercialName,
) -> float:
    """
    Calculate the Macaulay duration of the NTN-B bond in business years.

    Args:
        settlement (DateScalar): The settlement date of the operation.
        maturity (DateScalar): The maturity date of the NTN-B bond.
        rate (float): The discount rate used to calculate the duration.
        commercial_name (CommercialName): The commercial name of the NTN-B1 bond
            (Renda+ or Educa+).

    Returns:
        float: The Macaulay duration of the NTN-B bond in business years.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.duration("23-06-2025", "15-12-2084", 0.0686, r_mais)
        47.10493458167134
    """
    # Return NaN if any input is nullable
    if has_nullable_args(settlement, maturity, rate, commercial_name):
        return float("nan")

    df = cash_flows(settlement, maturity, commercial_name)
    s_byears = bday.count(settlement, df["PaymentDate"]) / 252
    s_dcf = df["CashFlow"] / (1 + rate) ** s_byears
    duration = (s_dcf * s_byears).sum() / s_dcf.sum()

    # Truncate duration to 14 decimal places for result reproducibility
    return bt.truncate(duration, 14)


def dv01(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    vna: float,
    commercial_name: CommercialName = CommercialName.RENDA_MAIS,
) -> float:
    """
    Calculate the DV01 (Dollar Value of 01) for an NTN-B1 in R$.

    Represents the price change in R$ for a 1 basis point (0.01%) increase in yield.

    Args:
        settlement (DateScalar): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (DateScalar): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-B.
        vna (float): The nominal value of the NTN-B bond.
        commercial_name (CommercialName): The commercial name of the NTN-B1 bond

    Returns:
        float: The DV01 value, representing the price change for a 1 basis point
            increase in yield.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.dv01("23-06-2025", "15-12-2084", 0.0686, 4299.160173, r_mais)
        0.7738490000000127
    """
    if has_nullable_args(settlement, maturity, rate, vna, commercial_name):
        return float("nan")

    quotation1 = quotation(settlement, maturity, rate, commercial_name)
    quotation2 = quotation(settlement, maturity, rate + 0.0001, commercial_name)
    price1 = price(vna, quotation1)
    price2 = price(vna, quotation2)
    return price1 - price2
