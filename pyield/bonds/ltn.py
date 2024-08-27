import pandas as pd

from .. import bday
from .. import date_converter as dc
from ..data import anbima, di
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
    ltn_rates = anbima.rates(reference_date, "LTN")
    if ltn_rates.empty:
        return pd.DataFrame()
    return ltn_rates[["MaturityDate", "IndicativeRate"]]


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


def premium(ltn_rate: float, di_rate: float) -> float:
    """
    Calculate the premium of the LTN bond over the DI Future rate using provided rates.

    Args:
        ltn_rate (float): The annualized LTN rate.
        di_future_rate (float): The annualized DI Future rate.

    Returns:
        float: The premium of the LTN bond over the DI Future rate.
    """
    # Cálculo das taxas diárias
    ltn_factor = (1 + ltn_rate) ** (1 / 252)
    di_factor = (1 + di_rate) ** (1 / 252)

    # Retorno do cálculo do prêmio
    return round((ltn_factor - 1) / (di_factor - 1), 6)


def historical_premium(
    reference_date: str | pd.Timestamp,
    maturity: str | pd.Timestamp,
) -> float:
    """
    Calculate the premium of the LTN bond over the DI Future rate for a given date.

    Args:
        reference_date (str | pd.Timestamp): The reference date to fetch the rates.
        maturity (str | pd.Timestamp): The maturity date of the LTN bond.

    Returns:
        float: The premium of the LTN bond over the DI Future rate for the given date.
               If the data is not available, returns NaN.
    """
    # Convert input dates to a consistent format
    reference_date = dc.convert_date(reference_date)
    maturity = dc.convert_date(maturity)

    # Retrieve LTN rates for the reference date
    df_anbima = rates(reference_date)
    if df_anbima.empty:
        return float("NaN")

    # Extract the LTN rate for the specified maturity date
    ltn_rates = df_anbima.query("MaturityDate == @maturity")["IndicativeRate"]
    if ltn_rates.empty:
        return float("NaN")
    ltn_rate = float(ltn_rates.iloc[0])

    # Retrieve DI rate for the reference date and maturity
    di_rate = di.rate(trade_date=reference_date, expiration=maturity)
    if pd.isnull(di_rate):  # Check if the DI rate is NaN
        return float("NaN")

    # Calculate and return the premium using the extracted rates
    return premium(ltn_rate, di_rate)
