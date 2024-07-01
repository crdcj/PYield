import pandas as pd

from . import bday
from . import date_validator as dv
from .fetchers.anbima import anbima


def coupon_dates_map(
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    adjust_for_bdays: bool = False,
) -> pd.Series:
    """
    Generate a map of all possible coupon dates between the start and end dates.
    The dates are inclusive. Coupon payments are made on the 1st of January and July.

    Args:
        start (str | pd.Timestamp): The start date.
        end (str | pd.Timestamp): The end date.
        adjust_for_bdays (bool, optional): If True, the coupon dates will be adjusted
            for business days. Defaults to False.

    Returns:
        pd.Series: Series of coupon dates within the specified range.
    """
    # Validate and normalize dates
    start = dv.normalize_date(start)
    end = dv.normalize_date(end)

    # Initialize the first coupon date based on the reference date
    reference_year = start.year
    first_coupon_date = pd.Timestamp(f"{reference_year}-01-01")

    # Generate coupon dates
    dates = pd.date_range(start=first_coupon_date, end=end, freq="6MS")

    # First coupon date must be after the reference date
    dates = dates[dates >= start]

    # Convert to Series and adjust for business days if necessary
    dates = pd.Series(dates).reset_index(drop=True)
    if adjust_for_bdays:
        dates = bday.offset(dates, 0)

    return dates


def anbima_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-F Anbima data for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the Anbima data for the reference date.
    """
    return anbima(bond_type="NTN-F", reference_date=reference_date)


def indicative_rates(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Fetch NTN-F Anbima indicative rates for the given reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for fetching the data.

    Returns:
        pd.DataFrame: A DataFrame containing the maturity dates and corresponding rates.
    """
    df = anbima_data(reference_date)

    # Keep only the relevant columns for the output
    keep_columns = ["ReferenceDate", "BondType", "MaturityDate", "IndicativeRate"]
    return df[keep_columns].copy()
