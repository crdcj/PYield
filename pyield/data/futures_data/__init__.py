import datetime as dt

import pandas as pd
from pytz import timezone

from ... import date_converter as dc
from .historical import fetch_historical_df
from .intraday import fetch_intraday_df

SUPPORTED_FUTURES = ["DI1", "DDI", "FRC", "DAP", "DOL", "WDO", "IND", "WIN"]
TIMEZONE_BZ = timezone("America/Sao_Paulo")


def futures(
    contract_code: str,
    trade_date: str | pd.Timestamp,
) -> pd.DataFrame:
    """
    Fetches data for a specified futures contract based on type and reference date.

    Args:
        contract_code (str): The B3 futures contract code identifying the derivative.
            Supported contract codes are:
            - "DI1": One-day Interbank Deposit Futures (Futuro de DI) from B3.
            - "DDI": DI x U.S. Dollar Spread Futures (Futuro de Cupom Cambial) from B3.
            - "FRC": Forward Rate Agreement (FRA) from B3.
            - "DAP": DI x IPCA Spread Futures.
            - "DOL": U.S. Dollar Futures from B3.
            - "WDO": Mini U.S. Dollar Futures from B3.
            - "IND": Ibovespa Futures from B3.
            - "WIN": Mini Ibovespa Futures from B3.
        reference_date (str | pd.Timestamp): The date for which to fetch the data.
            If the reference date is a string, it should be in 'DD-MM-YYYY' format.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched data for the specified futures
            contract.

    Raises:
        ValueError: If the futures contract code is not recognized or supported.

    Examples:
        >>> futures("DI1", "31-05-2024")
        >>> futures("DDI", "31-05-2024")
    """
    contract_code = contract_code.upper()
    if contract_code not in SUPPORTED_FUTURES:
        raise ValueError("Futures contract not supported.")

    trade_date = dc.convert_date(trade_date)

    # First, try to fetch historical data for the specified date
    df = fetch_historical_df(contract_code, trade_date)

    bz_today = dt.datetime.now(TIMEZONE_BZ).date()
    # If there is no historical data available, try to fetch intraday data
    if trade_date.date() == bz_today and df.empty:
        df = fetch_intraday_df(contract_code)

    return df


__all__ = ["futures"]
