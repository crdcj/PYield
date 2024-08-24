import pandas as pd

from .. import bday
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
