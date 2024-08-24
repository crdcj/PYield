import pandas as pd

from .. import bday
from .. import date_converter as dc
from ..data import anbima
from . import bond_tools as bt

FACE_VALUE = 1000


def rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the LTN Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame containing the maturity dates and indicative rates
            for LTN bonds.
    """
    return anbima.rates(reference_date, "LTN")[["MaturityDate", "IndicativeRate"]]


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


def price(
    settlement: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
    rate: float,
) -> float:
    """
    Calculate the LTN price using Anbima rules.

    Args:
        settlement (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-F.

    Returns:
        float: The LTN price using Anbima rules.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> price("05-07-2024", "01-01-2030", 0.12145)
        535.279902
    """

    # Validate and normalize dates
    settlement = dc.convert_date(settlement)
    maturity = dc.convert_date(maturity)

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement, maturity)

    # Calculate the number of periods truncated as per Anbima rule
    num_of_years = bt.truncate(bdays / 252, 14)

    discount_factor = (1 + rate) ** num_of_years

    # Truncate the price to 6 decimal places as per Anbima rules
    return bt.truncate(FACE_VALUE / discount_factor, 6)


def di_spreads(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Calculates the DI spread for the LTN based on ANBIMA's indicative rates.

    This function fetches the indicative rates for the NTN-F bonds and the DI futures
    rates and calculates the spread between these rates in basis points.

    Parameters:
        reference_date (str | pd.Timestamp, optional): The reference date for the
            spread calculation. If None or not provided, defaults to the previous
            business day according to the Brazilian calendar.

    Returns:
        pd.Series: A pandas series containing the calculated spreads in basis points
            indexed by maturity dates.
    """
    # Fetch DI Spreads for the reference date
    df = bt.di_spreads(reference_date)
    df.query("BondType == 'LTN'", inplace=True)
    df.sort_values(["MaturityDate"], ignore_index=True, inplace=True)
    return df[["MaturityDate", "DISpread"]]
