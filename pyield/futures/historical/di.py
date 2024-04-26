import pandas as pd

from ... import bday as bd
from . import common as cm


def _convert_prices_to_rates(prices: pd.Series, bd: pd.Series) -> pd.Series:
    """
    Internal function to convert DI futures prices to rates.

    Args:
        prices (pd.Series): A pd.Series containing DI futures prices.
        bd (pd.Series): A serie containing the number of business days to expiration.

    Returns:
        pd.Series: A pd.Series containing DI futures rates.
    """
    rates = (100_000 / prices) ** (252 / bd) - 1

    # Round to 5 (3 in %) dec. places (contract's current max. precision)
    return rates.round(5)


def _adjust_older_contracts_rates(df: pd.DataFrame, rate_cols: list) -> pd.DataFrame:
    for col in rate_cols:
        df[col] = _convert_prices_to_rates(df[col], df["BDaysToExp"])

    # Invert low and high prices
    df["MinRate"], df["MaxRate"] = df["MaxRate"], df["MinRate"]

    return df


def _process_raw_df(df: pd.DataFrame, trade_date: pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to process and transform raw DI futures data.

    Args:
        df (pd.DataFrame): the raw DI DataFrame.
        trade_date: a datetime-like object representing the trade date.

    Returns:
        pd.DataFrame: Processed and transformed data as a Pandas pd.DataFrame.
    """
    df["BDaysToExp"] = bd.count_bdays(trade_date, df["ExpirationDate"])

    # Remove expired contracts
    df.query("BDaysToExp > 0", inplace=True)

    rate_cols = [col for col in df.columns if "Rate" in col]
    # Prior to 17/01/2002 (inclusive), prices were not converted to rates
    if trade_date > pd.Timestamp("2002-01-17"):
        # Remove % and round to 5 (3 in %) dec. places in rate columns
        df[rate_cols] = df[rate_cols].div(100).round(5)
    else:
        df = _adjust_older_contracts_rates(df, rate_cols)

    # Calculate SettlementRate
    df["SettlementRate"] = _convert_prices_to_rates(
        df["SettlementPrice"], df["BDaysToExp"]
    )

    return df


def fetch_di(trade_date: pd.Timestamp, return_raw: bool = False) -> pd.DataFrame:
    """
    Fetchs the DI futures data for a given date from B3.

    This function fetches and processes the DI futures data from B3 for a specific
    trade date. It's the primary external interface for accessing DI data. If the data
    is not available, an empty DataFrame is returned.

    Args:
        trade_date (pd.Timestamp): The trade date to fetch the DI futures data.
        return_raw (bool): If True, returns the raw data as a Pandas pd.DataFrame.
            Defaults to False.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing processed DI futures data.

    Notes:
        - BDaysToExp: number of business days to ExpirationDate.
        - OpenContracts: number of open contracts at the start of the trading day.
    """
    df_raw = cm.fetch_raw_df(asset_code="DI1", trade_date=trade_date)
    if return_raw or df_raw.empty:
        return df_raw
    df = cm.pre_process_raw_df(df_raw, trade_date, asset_code="DI1")
    df = _process_raw_df(df, trade_date)
    # Filter and order columns
    df = cm.reorder_columns(df)
    return df
