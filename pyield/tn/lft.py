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
           ReferenceDate BondType  SelicCode  ...   AskRate IndicativeRate    DIRate
        0     2024-08-23      LFT     210100  ...  0.000226       0.000272   0.10408
        1     2024-08-23      LFT     210100  ... -0.000481      -0.000418   0.11082
        2     2024-08-23      LFT     210100  ... -0.000258       -0.00023  0.114315
        3     2024-08-23      LFT     210100  ...   0.00006       0.000075  0.114982
        ...
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
