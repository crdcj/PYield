import polars as pl

import pyield.converters as cv
from pyield import anbima, bday
from pyield.tn import tools
from pyield.types import DateScalar, has_null_args


def data(date: DateScalar) -> pl.DataFrame:
    """
    Fetch the LFT indicative rates for the given reference date from ANBIMA.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pl.DataFrame: DataFrame containing the following columns:
            - ReferenceDate: The reference date for the data.
            - BondType: The type of bond.
            - MaturityDate: The maturity date of the LFT bond.
            - IndicativeRate: The Anbima indicative rate for the LFT bond.
            - Price: The price of the LFT bond.

    Examples:
        >>> from pyield import lft
        >>> lft.data("23-08-2024")
        shape: (14, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬───────────┬───────────┬────────────────┬──────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate   ┆ AskRate   ┆ IndicativeRate ┆ DIRate   │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---       ┆ ---       ┆ ---            ┆ ---      │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64       ┆ f64       ┆ f64            ┆ f64      │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪═══════════╪═══════════╪════════════════╪══════════╡
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.000306  ┆ 0.000226  ┆ 0.000272       ┆ 0.10408  │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ -0.000397 ┆ -0.000481 ┆ -0.000418      ┆ 0.11082  │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ -0.000205 ┆ -0.000258 ┆ -0.00023       ┆ 0.114315 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.000085  ┆ 0.00006   ┆ 0.000075       ┆ 0.114982 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.000124  ┆ 0.000097  ┆ 0.000114       ┆ 0.114955 │
        │ …             ┆ …        ┆ …         ┆ …             ┆ … ┆ …         ┆ …         ┆ …              ┆ …        │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001501  ┆ 0.001476  ┆ 0.001491       ┆ 0.11564  │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001597  ┆ 0.001571  ┆ 0.001587       ┆ 0.115773 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001601  ┆ 0.001574  ┆ 0.001591       ┆ 0.115904 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001649  ┆ 0.001627  ┆ 0.001641       ┆ 0.115854 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001696  ┆ 0.00168   ┆ 0.001687       ┆ 0.115806 │
        └───────────────┴──────────┴───────────┴───────────────┴───┴───────────┴───────────┴────────────────┴──────────┘
    """  # noqa: E501
    return anbima.tpf_data(date, "LFT")


def maturities(date: DateScalar) -> pl.Series:
    """
    Fetch the bond maturities available for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pl.Series: A Series of bond maturities available for the reference date.

    Examples:
        >>> from pyield import lft
        >>> lft.maturities("22-08-2024")
        shape: (14,)
        Series: 'MaturityDate' [date]
        [
            2024-09-01
            2025-03-01
            2025-09-01
            2026-03-01
            2026-09-01
            …
            2029-03-01
            2029-09-01
            2030-03-01
            2030-06-01
            2030-09-01
        ]
    """
    df_rates = data(date)
    return df_rates["MaturityDate"]


def quotation(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float | None:
    """
    Calculate the quotation of a LFT bond using Anbima rules.

    Args:
        settlement (DateScalar): The settlement date of the bond.
        maturity (DateScalar): The maturity date of the bond.
        rate (float): The annualized yield rate of the bond

    Returns:
        float | None: The quotation of the bond.

    Examples:
        Calculate the quotation of a LFT bond with a 0.02 yield rate:
        >>> from pyield import lft
        >>> lft.quotation(
        ...     settlement="24-07-2024",
        ...     maturity="01-09-2030",
        ...     rate=0.001717,  # 0.1717%
        ... )
        98.9645
    """
    # Validate and normalize dates
    if has_null_args(settlement, maturity, rate):
        return None
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    # The number of bdays between settlement (inclusive) and the maturity (exclusive)
    bdays = bday.count(settlement, maturity)

    # Calculate the number of periods truncated as per Anbima rules
    num_of_years = tools.truncate(bdays / 252, 14)

    discount_factor = 1 / (1 + rate) ** num_of_years

    return tools.truncate(100 * discount_factor, 4)


def premium(lft_rate: float, di_rate: float) -> float | None:
    """
    Calculate the premium of the LFT bond over the DI Futures rate.

    Args:
        lft_rate (float): The annualized trading rate over the selic rate for the bond.
        di_rate (float): The DI Futures annualized yield rate (interpolated to the same
            maturity as the LFT).

    Returns:
        float | None: The premium of the LFT bond over the DI Futures rate.

    Examples:
        Calculate the premium of a LFT in 28/04/2025
        >>> from pyield import lft
        >>> lft_rate = 0.001124  # 0.1124%
        >>> di_rate = 0.13967670224373396  # 13.967670224373396%
        >>> lft.premium(lft_rate, di_rate)
        1.008594331960501
    """
    if has_null_args(lft_rate, di_rate):
        return None
    # daily rate
    ltt_factor = (lft_rate + 1) ** (1 / 252)
    di_factor = (di_rate + 1) ** (1 / 252)
    return (ltt_factor * di_factor - 1) / (di_factor - 1)
