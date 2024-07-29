import pandas as pd

from .. import bday
from .. import date_converter as dc
from .. import fetchers as ft
from . import utils as ut


def anbima_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch LFT Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return ft.anbima_data(reference_date, "LFT")


def anbima_rates(reference_date: str | pd.Timestamp) -> pd.Series:
    """
    Fetch LFT Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.Series: A Series containing the rates indexed by maturity date.
    """
    return ut.get_anbima_rates(reference_date, "LFT")


def anbima_historical_rates(maturity_date: str | pd.Timestamp) -> pd.Series:
    """
    Fetch historical LFT Anbima indicative rates for the given maturity date.

    Args:
        maturity_date (str | pd.Timestamp): The maturity date of the bond.

    Returns:
        pd.Series: A Series containing the rates indexed by reference date.
    """
    return ut.get_anbima_historical_rates("LFT", maturity_date)


def quote(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
    yield_rate: float,
) -> float:
    """
    Calculate the quote of a LFT bond using Anbima rules.

    Args:
        settlement_date (str | pd.Timestamp): The settlement date of the bond.
        maturity_date (str | pd.Timestamp): The maturity date of the bond.
        yield_rate (float): The annualized yield of the bond

    Returns:
        float: The quote of the bond.

    Examples:
        Calculate the quote of a LFT bond with a 0.02 yield rate:
        >>> lft.quote(
        ...     settlement_date="24-07-2024",
        ...     maturity_date="01-09-2030",
        ...     yield_rate=0.001717,  # 0.1717%
        ... )
        98.9645
    """
    # Validate and normalize dates
    settlement_date = dc.convert_date(settlement_date)
    maturity_date = dc.convert_date(maturity_date)

    # The number of bdays between settlement (inclusive) and the maturity (exclusive)
    bdays = bday.count(settlement_date, maturity_date)

    # Calculate the number of periods truncated as per Anbima rules
    num_of_years = ut.truncate(bdays / 252, 14)

    discount_factor = 1 / (1 + yield_rate) ** num_of_years

    return ut.truncate(100 * discount_factor, 4)
