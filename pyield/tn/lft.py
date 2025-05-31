import pandas as pd

from pyield import anbima, bday
from pyield import date_converter as dc
from pyield.date_converter import DateScalar
from pyield.tn import tools


def data(date: DateScalar) -> pd.DataFrame:
    """
    Fetch the LFT indicative rates for the given reference date from ANBIMA.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame containing the following columns:
            - ReferenceDate: The reference date for the data.
            - BondType: The type of bond.
            - MaturityDate: The maturity date of the LFT bond.
            - IndicativeRate: The Anbima indicative rate for the LFT bond.
            - Price: The price of the LFT bond.

    Examples:
        >>> from pyield import lft
        >>> lft.data("23-08-2024")
           ReferenceDate BondType MaturityDate  IndicativeRate         Price
        0     2024-08-23      LFT   2024-09-01        0.000272  15252.158852
        1     2024-08-23      LFT   2025-03-01       -0.000418  15255.605864
        2     2024-08-23      LFT   2025-09-01        -0.00023  15255.819395
        3     2024-08-23      LFT   2026-03-01        0.000075  15250.526859
        4     2024-08-23      LFT   2026-09-01        0.000114  15248.757596
        5     2024-08-23      LFT   2027-03-01        0.000669  15226.824838
        6     2024-08-23      LFT   2027-09-01        0.000948  15208.842417
        7     2024-08-23      LFT   2028-03-01        0.001172  15189.853347
        8     2024-08-23      LFT   2028-09-01        0.001328  15171.352348
        9     2024-08-23      LFT   2029-03-01        0.001491  15150.700781
        10    2024-08-23      LFT   2029-09-01        0.001587  15131.894737
        11    2024-08-23      LFT   2030-03-01        0.001591  15119.952213
        12    2024-08-23      LFT   2030-06-01        0.001641  15109.717943
        13    2024-08-23      LFT   2030-09-01        0.001687  15099.285393
    """
    return anbima.tpf_data(date, "LFT")


def quotation(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the quotation of a LFT bond using Anbima rules.

    Args:
        settlement (DateScalar): The settlement date of the bond.
        maturity (DateScalar): The maturity date of the bond.
        rate (float): The annualized yield rate of the bond

    Returns:
        float: The quotation of the bond.

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
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

    # The number of bdays between settlement (inclusive) and the maturity (exclusive)
    bdays = bday.count(settlement, maturity)

    # Calculate the number of periods truncated as per Anbima rules
    num_of_years = tools.truncate(bdays / 252, 14)

    discount_factor = 1 / (1 + rate) ** num_of_years

    return tools.truncate(100 * discount_factor, 4)


def premium(lft_rate: float, di_rate: float) -> float:
    """
    Calculate the premium of the LFT bond over the DI Futures rate.

    Args:
        lft_rate (float): The annualized trading rate over the selic rate for the bond.
        di_rate (float): The DI Futures annualized yield rate (interpolated to the same
            maturity as the LFT).

    Returns:
        float: The premium of the LFT bond over the DI Futures rate.

    Examples:
        Calculate the premium of a LFT in 28/04/2025
        >>> from pyield import lft
        >>> lft_rate = 0.001124  # 0.1124%
        >>> di_rate = 0.13967670224373396  # 13.967670224373396%
        >>> lft.premium(lft_rate, di_rate)
        1.008594331960501
    """
    # daily rate
    ltt_factor = (lft_rate + 1) ** (1 / 252)
    di_factor = (di_rate + 1) ** (1 / 252)
    return (ltt_factor * di_factor - 1) / (di_factor - 1)
