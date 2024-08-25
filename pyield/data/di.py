import pandas as pd

from .. import bday, interpolator
from .. import date_converter as dc
from .data_cache import get_anbima_dataframe, get_di_dataframe
from .futures_data import futures


def data(
    trade_date: str | pd.Timestamp,
    adj_expirations: bool = False,
    prefixed_filter: bool = False,
) -> pd.DataFrame:
    """Retrieve DI contract data for a specified trade date.

    This function retrieves DI contract data for the given trade date. If the historical
    data is not available, it attempts to fetch the data from the B3 website. The
    function can also filter and adjust expiration dates based on the provided options.

    Args:
        trade_date (str | pd.Timestamp): The trade date for which the DI data is
            required.
        adj_expirations (bool): If True, adjusts the expiration dates to the
            start of the month.
        prefixed_filter (bool): If True, filters the DI contracts to match prefixed
            Anbima bond maturities.

    Returns:
        pd.DataFrame: A DataFrame containing DI contract data, with columns for
            expiration dates and selected rates. The DataFrame is sorted by the
            expiration date.
    """
    trade_date = dc.convert_date(trade_date)
    df = get_di_dataframe()
    df.query("TradeDate == @trade_date", inplace=True)

    if df.empty:
        # There is no historical data for date provided.
        # Let's try to fetch the data from the B3 website.
        df = futures(contract_code="DI1", trade_date=trade_date)
    if df.empty:
        # If it is still empty, return an empty DataFrame.
        return pd.DataFrame()

    df.drop(columns=["TradeDate"], inplace=True)
    if "DaysToExpiration" in df.columns:
        df.drop(columns=["DaysToExpiration"], inplace=True)

    if prefixed_filter:
        df_anb = get_anbima_dataframe()
        df_anb.query("ReferenceDate == @trade_date", inplace=True)
        df_pre = df_anb.query("BondType in ['LTN', 'NTN-F']").copy()
        pre_maturities = df_pre["MaturityDate"].drop_duplicates(ignore_index=True)
        adj_pre_maturities = bday.offset(pre_maturities, 0)  # noqa
        df = df.query("ExpirationDate in @adj_pre_maturities")

    if adj_expirations:
        df["ExpirationDate"] = df["ExpirationDate"].dt.to_period("M")
        df["ExpirationDate"] = df["ExpirationDate"].dt.to_timestamp()

    return df.sort_values(["ExpirationDate"], ignore_index=True)


def expirations(
    trade_date: str | pd.Timestamp,
    adj_expirations: bool = False,
    prefixed_filter: bool = False,
) -> pd.Series:
    """Retrieve unique expiration dates for DI contracts on a specified trade date.

    This function returns a Series of unique expiration dates for DI contracts
    for the given trade date. The expiration dates can be adjusted or filtered
    based on the provided options.

    Args:
        trade_date (str | pd.Timestamp): The trade date for which expiration dates are
            required.
        adj_expirations (bool): If True, adjusts the expiration dates to the start of
            the month.
        prefixed_filter (bool): If True, filters the DI contracts to match prefixed
            Anbima bond maturities.

    Returns:
        pd.Series: A Series of unique expiration dates for DI contracts.
    """
    trade_date = dc.convert_date(trade_date)
    df = data(trade_date, adj_expirations, prefixed_filter)
    df = df.drop_duplicates(subset=["ExpirationDate"], ignore_index=True)
    return df["ExpirationDate"]


def rate(
    trade_date: str | pd.Timestamp,
    expiration: str | pd.Timestamp,
    interpolate: bool = True,
) -> float:
    """Retrieve the DI rate for a specified trade date and expiration date.

    This function returns the DI rate for the given trade date and expiration date.
    The function uses the SettlementRate column from the DI contract data and
    interpolates the rate using the flat forward method if required.

    Args:
        trade_date (str | pd.Timestamp): The trade date for which the DI rate is
            required.
        expiration (str | pd.Timestamp): The expiration date for the DI contract.
        interpolate (bool): If True, interpolates the rate for the provided expiration
            date.

    Returns:
        float: The DI rate for the specified trade date and expiration date, or NaN
               if the rate cannot be determined.
    """
    # Convert input dates to consistent format
    trade_date = dc.convert_date(trade_date)
    # Adjust expiration date to the nearest business day
    expiration = bday.offset(expiration, 0)

    # Retrieve the data for the given trade date
    df = data(trade_date)

    # Return NaN if no data is found for the trade date
    if df.empty:
        return float("NaN")

    # Filter data for the specified expiration date
    df_exp = df.query("ExpirationDate == @expiration")

    # Return NaN if no exact match is found and interpolation is not allowed
    if df_exp.empty and not interpolate:
        return float("NaN")

    if expiration in df_exp["ExpirationDate"]:
        # Return the rate if an exact match is found
        return float(df_exp["SettlementRate"].iloc[0])

    # Perform flat forward interpolation if required
    ff_interpolator = interpolator.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(trade_date, df["ExpirationDate"]),
        known_rates=df["SettlementRate"],
    )

    # Return the interpolated rate for the calculated business days
    return ff_interpolator(bday.count(trade_date, expiration))
