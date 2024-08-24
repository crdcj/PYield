import pandas as pd

from .. import bday
from .. import date_converter as dc
from ..data import anbima as an
from . import bond_tools as bt


def rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the bond Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame containing the maturity dates and indicative rates
            for the bonds.
    """
    return an.rates(reference_date, "NTN-F")[["MaturityDate", "IndicativeRate"]]


def maturities(reference_date: str | pd.Timestamp) -> pd.Series:
    """
    Fetch the bond maturities available for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.Series: A Series of bond maturities available for the reference date.
    """
    df_rates = rates(reference_date)
    return df_rates["MaturityDate"]


def quotation(
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
    rate: float,
) -> float:
    """
    Calculate the quotation of a LFT bond using Anbima rules.

    Args:
        settlement (str | pd.Timestamp): The settlement date of the bond.
        maturity (str | pd.Timestamp): The maturity date of the bond.
        rate (float): The annualized yield rate of the bond

    Returns:
        float: The quotation of the bond.

    Examples:
        Calculate the quotation of a LFT bond with a 0.02 yield rate:
        >>> lft.quotation(
        ...     settlement="24-07-2024",
        ...     maturity="01-09-2030",
        ...     rate=0.001717,  # 0.1717%
        ... )
        98.9645
    """
    # Validate and normalize dates
    settlement = dc.convert_date(settlement)
    maturity = dc.convert_date(maturity)

    # The number of bdays between settlement (inclusive) and the maturity (exclusive)
    bdays = bday.count(settlement, maturity)

    # Calculate the number of periods truncated as per Anbima rules
    num_of_years = bt.truncate(bdays / 252, 14)

    discount_factor = 1 / (1 + rate) ** num_of_years

    return bt.truncate(100 * discount_factor, 4)
