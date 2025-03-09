import pandas as pd

from pyield import bday
from pyield import date_converter as dc
from pyield.anbima import tpf
from pyield.date_converter import DateScalar
from pyield.tpf import tools as tt


def rates(date: DateScalar) -> pd.DataFrame:
    """
    Fetch the bond Anbima indicative rates for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame with columns "MaturityDate" and "IndicativeRate".
    """
    lft_rates = tpf.tpf_rates(date, "LFT")
    if lft_rates.empty:
        return pd.DataFrame()
    return lft_rates[["MaturityDate", "IndicativeRate"]]


def maturities(date: DateScalar) -> pd.Series:
    """
    Fetch the bond maturities available for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.Series: A Series of bond maturities available for the reference date.
    """
    df_rates = rates(date)
    return df_rates["MaturityDate"]


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
        >>> yd.lft.quotation(
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
    num_of_years = tt.truncate(bdays / 252, 14)

    discount_factor = 1 / (1 + rate) ** num_of_years

    return tt.truncate(100 * discount_factor, 4)


def premium(lft_rate: float, selic_over: float) -> float:
    """
    Calculate the premium of the LFT bond over the Selic rate (overnight).
    Obs: The Selic rate (overnight) is not the same as the Selic target rate

    Args:
        lft_rate (float): The LFT rate for the bond.
        selic_over (float): The Selic overnight rate.

    Returns:
        float: The premium of the LFT bond over the Selic rate.

    Examples:
        Calculate the premium of a LFT bond with a 0.02 yield rate over the Selic rate:
        >>> lft_rate = 0.1695 / 100  # 0.1695%
        >>> selic_over = 10.40 / 100  # 10.40%
        >>> yd.lft.premium(lft_rate, selic_over)
        1.017120519283759
    """
    adjusted_lft_rate = (lft_rate + 1) * (selic_over + 1) - 1
    f1 = (adjusted_lft_rate + 1) ** (1 / 252) - 1
    f2 = (selic_over + 1) ** (1 / 252) - 1
    return f1 / f2
