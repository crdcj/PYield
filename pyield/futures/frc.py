import pandas as pd

from . import common


def _process_past_data(df: pd.DataFrame, trade_date: pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to process and transform raw DI futures data.

    Args:
        df (pd.DataFrame): the raw DI DataFrame.
        trade_date: a datetime-like object representing the trade date.

    Returns:
        pd.DataFrame: Processed and transformed data as a Pandas pd.DataFrame.
    """
    # Check if the pd.DataFrame is empty
    if df.empty:
        return df

    rename_dict = {
        "VENCTO": "ExpirationCode",
        "NÚM. NEGOC.": "TradeCount",
        "CONTR. NEGOC.": "TradeVolume",
        "VOL.": "FinancialVolume",
        "AJUSTE  DE REF.": "SettlementRate",
        "PREÇO ABERTU.": "FirstRate",
        "PREÇO MÍN.": "MinRate",
        "PREÇO MÉD.": "AvgRate",
        "PREÇO MÁX.": "MaxRate",
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

    df["ExpirationDate"] = df["ExpirationCode"].apply(common.get_expiration_date)

    # Replace 0 values in rate columns with pd.NA and remove percentage
    rate_cols = [col for col in df.columns if "Rate" in col]
    for col in rate_cols:
        df[col] = df[col].replace(0, pd.NA)
        # Round to 5 decimal places (3 in %) since it is the contract's precision
        df[col] = (df[col] / 100).round(5)

    df["TickerSymbol"] = "FRC" + df["ExpirationCode"]

    # Filter and order columns
    ordered_cols = [
        "TradeDate",
        "TickerSymbol",
        # "ExpirationCode",
        "ExpirationDate",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
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


def fetch_past_frc(trade_date: pd.Timestamp, return_raw: bool = False) -> pd.DataFrame:
    """
    Fetchs the DI futures data for a given date from B3.

    This function fetches and processes the DI futures data from B3 for a specific
    trade date. It's the primary external interface for accessing DI data.

    Args:
        trade_date (pd.Timestamp): The trade date to fetch the DI futures data.
        raw (bool): If True, returns the raw data as a Pandas pd.DataFrame.
            Defaults to False.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing processed DI futures data.
    """
    df_raw = common.fetch_past_raw_df(asset_code="FRC", trade_date=trade_date)
    if return_raw:
        return df_raw
    return _process_past_data(df_raw, trade_date)
