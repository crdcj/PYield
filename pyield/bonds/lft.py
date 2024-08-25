import pandas as pd

from .. import bday
from .. import date_converter as dc
from ..data import anbima, di
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
    lft_rates = anbima.rates(reference_date, "LFT")
    if lft_rates.empty:
        return pd.DataFrame()
    return lft_rates[["MaturityDate", "IndicativeRate"]]


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


def premium(lft_rate: float, di_rate: float) -> float:
    di_factor = (1 + di_rate) ** (1 / 252)
    lft_factor = (1 + lft_rate) ** (1 / 252) * di_factor

    return (lft_factor - 1) / (di_factor - 1)


def historical_premium(
    reference_date: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
) -> float:
    """
    Calculate the premium of the LFT bond over the DI Future rate for a given date.

    Args:
        reference_date (str | pd.Timestamp): The reference date to fetch the rates.
        maturity (str | pd.Timestamp): The maturity date of the LFT bond.

    Returns:
        float: The premium of the LFT bond over the DI Future rate for the given date.
               If the data is not available, returns NaN.
    """
    # Convert input dates to a consistent format
    reference_date = dc.convert_date(reference_date)
    maturity = dc.convert_date(maturity)

    # Retrieve LFT rates for the reference date
    df_anbima = rates(reference_date)
    if df_anbima.empty:
        return float("NaN")

    # Extract the LFT rate for the specified maturity date
    lft_rates = df_anbima.query("MaturityDate == @maturity")["IndicativeRate"]
    if lft_rates.empty:
        return float("NaN")
    lft_rate = float(lft_rates.iloc[0])

    # Retrieve DI rate for the reference date and maturity
    di_rate = di.rate(trade_date=reference_date, expiration=maturity)
    if pd.isnull(di_rate):  # Check if the DI rate is NaN
        return float("NaN")

    # Calculate and return the premium using the extracted rates
    return premium(lft_rate, di_rate)
