import pandas as pd

from . import common as cm


def _convert_prices_to_rates(prices: pd.Series, n_days: pd.Series) -> pd.Series:
    """
    Internal function to convert DDI futures prices to rates.

    Args:
        prices (pd.Series): A pd.Series containing DDI futures prices.
        bd (pd.Series): A serie containing the number of days to expiration.

    Returns:
        pd.Series: A pd.Series containing DDI futures rates.
    """
    rates = (100_000 / prices - 1) * (360 / n_days)
    # Round to 5 (3 in %) dec. places (contract's current max. precision)
    return rates.round(5)


def _process_raw_df(df: pd.DataFrame, trade_date: pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to process and transform raw DI futures data.

    Args:
        df (pd.DataFrame): the raw DI DataFrame.
        trade_date: a datetime-like object representing the trade date.

    Returns:
        pd.DataFrame: Processed and transformed data as a Pandas pd.DataFrame.
    """
    df["DaysToExp"] = (df["ExpirationDate"] - trade_date).dt.days
    # Convert to nullable integer, since it is the default type in the library
    df["DaysToExp"] = df["DaysToExp"].astype(pd.Int64Dtype())

    # Remove expired contracts
    df.query("DaysToExp > 0", inplace=True)

    # Columns where 0 means NaN
    rate_cols = [col for col in df.columns if "Rate" in col]
    df[rate_cols] = df[rate_cols].div(100).round(5)

    # Calculate SettlementRate
    df["SettlementRate"] = _convert_prices_to_rates(
        df["SettlementPrice"], df["DaysToExp"]
    )

    return df


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
    if return_raw or df_raw.empty:
        return df_raw
    df = cm.pre_process_raw_df(df_raw, trade_date, asset_code="DDI")
    df = _process_raw_df(df, trade_date)
    return cm.reorder_columns(df)
