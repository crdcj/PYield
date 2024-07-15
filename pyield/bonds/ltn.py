import pandas as pd

from .. import bday
from .. import date_validator as dv
from ..fetchers.anbima import anbima
from .utils import truncate

FACE_VALUE = 1000


def anbima_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch LTN Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return anbima(bond_type="LTN", reference_date=reference_date)


def anbima_rates(reference_date: str | pd.Timestamp) -> pd.Series:
    """
    Fetch LTN Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.Series: A Series containing the rates indexed by maturity date.
    """
    df = anbima_data(reference_date)
    # Set MaturityDate as index
    df = df.set_index("MaturityDate")
    df.index.name = None
    # Return as Series
    return df["IndicativeRate"]


def price(
    settlement_date: str | pd.Timestamp,
    maturity_date: str | pd.Timestamp,
    discount_rate: float,
) -> float:
    """
    Calculate the LTN price using Anbima rules.

    Args:
        settlement_date (str | pd.Timestamp): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity_date (str | pd.Timestamp): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        discount_rate (float): The discount rate used to calculate the present value of
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
    settlement_date = dv.standardize_date(settlement_date)
    maturity_date = dv.standardize_date(maturity_date)

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement_date, maturity_date)

    # Calculate the number of periods truncated as per Anbima rule
    num_periods = truncate(bdays / 252, 14)

    discount_factor = (1 + discount_rate) ** num_periods

    # Truncate the price to 6 decimal places as per Anbima rules
    return truncate(FACE_VALUE / discount_factor, 6)
