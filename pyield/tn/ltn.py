import polars as pl

import pyield.converters as cv
from pyield import anbima, bday
from pyield.tn import tools
from pyield.types import DateScalar, has_null_args

FACE_VALUE = 1000


def data(date: DateScalar) -> pl.DataFrame:
    """
    Fetch the LTN Anbima indicative rates for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pl.DataFrame: DataFrame with columns "MaturityDate" and "IndicativeRate".

    Examples:
        >>> from pyield import ltn
        >>> ltn.data("23-08-2024")
        shape: (13, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬──────────┬──────────┬────────────────┬─────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate  ┆ AskRate  ┆ IndicativeRate ┆ DIRate  │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---      ┆ ---      ┆ ---            ┆ ---     │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64      ┆ f64      ┆ f64            ┆ f64     │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪══════════╪══════════╪════════════════╪═════════╡
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2022-07-08    ┆ … ┆ 0.10459  ┆ 0.104252 ┆ 0.104416       ┆ 0.10472 │
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2018-02-01    ┆ … ┆ 0.107366 ┆ 0.107016 ┆ 0.107171       ┆ 0.10823 │
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2023-01-06    ┆ … ┆ 0.110992 ┆ 0.110746 ┆ 0.110866       ┆ 0.11179 │
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2022-01-07    ┆ … ┆ 0.11315  ┆ 0.112947 ┆ 0.113032       ┆ 0.11365 │
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2023-07-07    ┆ … ┆ 0.114494 ┆ 0.114277 ┆ 0.114374       ┆ 0.11463 │
        │ …             ┆ …        ┆ …         ┆ …             ┆ … ┆ …        ┆ …        ┆ …              ┆ …       │
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2024-07-05    ┆ … ┆ 0.115424 ┆ 0.115283 ┆ 0.115357       ┆ 0.11494 │
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2023-07-07    ┆ … ┆ 0.115452 ┆ 0.115247 ┆ 0.115335       ┆ 0.11498 │
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2024-01-05    ┆ … ┆ 0.115758 ┆ 0.115633 ┆ 0.115694       ┆ 0.11508 │
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2024-07-05    ┆ … ┆ 0.11647  ┆ 0.116341 ┆ 0.116417       ┆ 0.11554 │
        │ 2024-08-23    ┆ LTN      ┆ 100000    ┆ 2024-01-05    ┆ … ┆ 0.117504 ┆ 0.11737  ┆ 0.117436       ┆ 0.11594 │
        └───────────────┴──────────┴───────────┴───────────────┴───┴──────────┴──────────┴────────────────┴─────────┘
    """  # noqa: E501
    return anbima.tpf_data(date, "LTN")


def maturities(date: DateScalar) -> pl.Series:
    """
    Fetch the bond maturities available for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pl.Series: A Series of bond maturities available for the reference date.

    Examples:
        >>> from pyield import ltn
        >>> ltn.maturities("22-08-2024")
        shape: (13,)
        Series: 'MaturityDate' [date]
        [
            2024-10-01
            2025-01-01
            2025-04-01
            2025-07-01
            2025-10-01
            …
            2026-10-01
            2027-07-01
            2028-01-01
            2028-07-01
            2030-01-01
        ]
    """
    df_rates = data(date)
    return df_rates["MaturityDate"]


def price(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the LTN price using Anbima rules.

    Args:
        settlement (DateScalar): The settlement date in 'DD-MM-YYYY' format
            or a date-like object.
        maturity (DateScalar): The maturity date in 'DD-MM-YYYY' format or
            a date-like object.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-F.

    Returns:
        float: The LTN price using Anbima rules.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ltn
        >>> ltn.price("05-07-2024", "01-01-2030", 0.12145)
        535.279902
    """
    # Validate and normalize inputs
    if has_null_args(settlement, maturity, rate):
        return float("nan")
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement, maturity)

    # Calculate the number of periods truncated as per Anbima rule
    num_of_years = tools.truncate(bdays / 252, 14)

    discount_factor = (1 + rate) ** num_of_years

    # Truncate the price to 6 decimal places as per Anbima rules
    return tools.truncate(FACE_VALUE / discount_factor, 6)


def premium(ltn_rate: float, di_rate: float) -> float:
    """
    Calculate the premium of the LTN bond over the DI Future rate using provided rates.

    Args:
        ltn_rate (float): The annualized LTN rate.
        di_rate (float): The annualized DI Future rate.

    Returns:
        float: The premium of the LTN bond over the DI Future rate.

    Examples:
        Reference date: 22-08-2024
        LTN rate for 01-01-2030: 0.118746
        DI (JAN30) Settlement rate: 0.11725
        >>> from pyield import ltn
        >>> ltn.premium(0.118746, 0.11725)
        1.0120718007994287
    """
    if has_null_args(ltn_rate, di_rate):
        return float("nan")
    # Cálculo das taxas diárias
    ltn_daily_rate = (1 + ltn_rate) ** (1 / 252) - 1
    di_daily_rate = (1 + di_rate) ** (1 / 252) - 1

    # Retorno do cálculo do prêmio
    return ltn_daily_rate / di_daily_rate


def dv01(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the DV01 (Dollar Value of 01) for an LTN in R$.

    Represents the price change in R$ for a 1 basis point (0.01%) increase in yield.

    Args:
        settlement (DateScalar): The settlement date in 'DD-MM-YYYY' format
            or a date-like object.
        maturity (DateScalar): The maturity date in 'DD-MM-YYYY' format or
            a date-like object.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the LTN.

    Returns:
        float: The DV01 value, representing the price change for a 1 basis point
            increase in yield.

    Examples:
        >>> from pyield import ltn
        >>> ltn.dv01("26-03-2025", "01-01-2032", 0.150970)
        0.2269059999999854
    """
    if has_null_args(settlement, maturity, rate):
        return float("nan")
    price1 = price(settlement, maturity, rate)
    price2 = price(settlement, maturity, rate + 0.0001)
    return price1 - price2
