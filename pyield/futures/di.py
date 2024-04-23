import pandas as pd

from .. import bday as bd
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


def _process_past_raw_df(df: pd.DataFrame, trade_date: pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to process and transform raw DI futures data.

    Args:
        df (pd.DataFrame): the raw DI DataFrame.
        trade_date: a datetime-like object representing the trade date.

    Returns:
        pd.DataFrame: Processed and transformed data as a Pandas pd.DataFrame.
    """
    rename_dict = {
        "VENCTO": "ExpirationCode",
        "CONTR. ABERT.(1)": "OpenContracts",  # At the start of the day
        "CONTR. FECH.(2)": "OpenContractsEndSession",  # At the end of the day
        "NÚM. NEGOC.": "TradeCount",
        "CONTR. NEGOC.": "TradeVolume",
        "VOL.": "FinancialVolume",
        "AJUSTE": "SettlementPrice",
        "AJUSTE ANTER. (3)": "PrevSettlementRate",
        "AJUSTE CORRIG. (4)": "AdjSettlementRate",
        "PREÇO MÍN.": "MinRate",
        "PREÇO MÉD.": "AvgRate",
        "PREÇO MÁX.": "MaxRate",
        "PREÇO ABERTU.": "FirstRate",
        "ÚLT. PREÇO": "CloseRate",
        "VAR. PTOS.": "PointsVariation",
        # Attention: bid/ask rates are inverted
        "ÚLT.OF. COMPRA": "CloseAskRate",
        "ÚLT.OF. VENDA": "CloseBidRate",
    }

    df = df.rename(columns=rename_dict)

    df["TradeDate"] = trade_date
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["TradeDate"] = df["TradeDate"].astype("datetime64[ns]")

    # Contract code format was changed in 22/05/2006
    if trade_date < pd.Timestamp("2006-05-22"):
        df["ExpirationDate"] = df["ExpirationCode"].apply(
            cm.get_old_expiration_date, args=(trade_date,)
        )
    else:
        df["ExpirationDate"] = df["ExpirationCode"].apply(cm.get_expiration_date)

    df["BDaysToExp"] = bd.count_bdays(trade_date, df["ExpirationDate"])

    # Remove expired contracts
    df.query("BDaysToExp > 0", inplace=True)

    # Columns where 0 means NaN
    rate_cols = [col for col in df.columns if "Rate" in col]
    cols_with_nan = rate_cols + ["SettlementPrice"]
    # Replace 0 with NaN in these columns
    df[cols_with_nan] = df[cols_with_nan].replace(0, pd.NA)

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

    df["TickerSymbol"] = "DI1" + df["ExpirationCode"]

    # Filter and order columns
    ordered_cols = [
        "TradeDate",
        "TickerSymbol",
        # "ExpirationCode",
        "ExpirationDate",
        "BDaysToExp",
        "OpenContracts",
        # "OpenContractsEndSession" since there is no OpenContracts at the end of the
        # day in XML data, it will be removed to avoid confusion with XML data
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "SettlementPrice",
        "SettlementRate",
        "FirstRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "CloseAskRate",
        "CloseBidRate",
        "CloseRate",
    ]
    return df[ordered_cols]


def _process_last_raw_di_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()

    # Columns to be renamed
    rename_columns = {
        "TradeTimestamp": "TradeTimestamp",
        "symb": "TickerSymbol",
        "mtrtyCode": "ExpirationDate",
        "BDaysToExp": "BDaysToExp",
        "opnCtrcts": "OpenContracts",
        "tradQty": "TradeCount",
        "traddCtrctsQty": "TradeVolume",
        "grssAmt": "FinancialVolume",
        "prvsDayAdjstmntPric": "PrevSettlementRate",
        "bottomLmtPric": "MinLimitRate",
        "topLmtPric": "MaxLimitRate",
        "opngPric": "OpenRate",
        "minPric": "MinRate",
        "avrgPric": "AvgRate",
        "maxPric": "MaxRate",
        "buyOffer.price": "LastAskRate",
        "sellOffer.price": "LastBidRate",
        "curPrc": "LastRate",
    }
    # Rename columns
    df = df.rename(columns=rename_columns)

    df["BDaysToExp"] = bd.count_bdays(df["TradeTimestamp"], df["ExpirationDate"])

    # Remove percentage in all rate columns
    rate_cols = [col for col in df.columns if "Rate" in col]
    df[rate_cols] = df[rate_cols] / 100

    # Reorder columns based on the order of the dictionary
    return df[rename_columns.values()]


def fetch_last_di() -> pd.DataFrame:
    """
    Fetch the latest DI futures data from B3.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing the latest DI futures data.
    """
    raw_df = cm.fetch_last_raw_df(future_code="DI1")
    return _process_last_raw_di_df(raw_df)


def fetch_past_di(trade_date: pd.Timestamp, return_raw: bool = False) -> pd.DataFrame:
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
    df_raw = cm.fetch_past_raw_df(asset_code="DI1", trade_date=trade_date)
    if return_raw:
        return df_raw
    if df_raw.empty:
        return pd.DataFrame()
    else:
        return _process_past_raw_df(df_raw, trade_date)
