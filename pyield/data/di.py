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
