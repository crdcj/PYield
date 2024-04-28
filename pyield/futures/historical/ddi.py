import pandas as pd

from . import common as cm


def fetch_ddi(trade_date: pd.Timestamp, return_raw: bool = False) -> pd.DataFrame:
    """
    Fetchs the DDI futures data for a given date from B3.

    This function fetches and processes the DI futures data from B3 for a specific
    trade date. It's the primary external interface for accessing DI data.

    Args:
        trade_date (pd.Timestamp): The trade date to fetch the DI futures data.
        raw (bool): If True, returns the raw data as a Pandas pd.DataFrame.
            Defaults to False.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing processed DI futures data.

    Examples:
        >>> from pyield.futures import di
        >>> di.fetch_ddi(pd.Timestamp("2021-01-04"))

    Notes:
        - DaysToExp: number of business days to ExpirationDate.
        - OpenContracts: number of open contracts at the start of the trading day.
    """
    df_raw = cm.fetch_raw_df(asset_code="DDI", trade_date=trade_date)
    if df_raw.empty:
        return df_raw
    df = cm.process_raw_df(df_raw, trade_date, asset_code="DDI", count_convention=360)
    return cm.reorder_columns(df)
