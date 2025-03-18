import datetime as dt
from typing import Literal
from zoneinfo import ZoneInfo

import pandas as pd

from pyield import date_converter as dc
from pyield.b3.futures.historical import fetch_historical_df
from pyield.b3.futures.intraday import fetch_intraday_df
from pyield.date_converter import DateScalar

TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")
ContractOptions = Literal["DI1", "DDI", "FRC", "DAP", "DOL", "WDO", "IND", "WIN"]


def futures(
    contract_code: ContractOptions | str,
    date: DateScalar,
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
        date (DateScalar): The date for which to fetch the data.
            If the reference date is a string, it should be in 'DD-MM-YYYY' format.

    Returns:
        pd.DataFrame: DataFrame containing the fetched data for the specified futures
            contract.

    Raises:
        ValueError: If the futures contract code is not recognized or supported.

    Examples:
        >>> futures("DI1", "31-05-2024")
            TradeDate TickerSymbol  ... CloseBidRate  CloseRate
        0  2024-05-31       DI1M24  ...      0.10404    0.10404
        1  2024-05-31       DI1N24  ...       0.1039    0.10386
        2  2024-05-31       DI1Q24  ...      0.10374    0.10374
        3  2024-05-31       DI1U24  ...      0.10365    0.10355
        ...
        >>> futures("DAP", "31-05-2024")
            TradeDate TickerSymbol  ... CloseBidRate  CloseRate
        0  2024-05-31       DAPM24  ...         <NA>       <NA>
        1  2024-05-31       DAPN24  ...         <NA>       <NA>
        2  2024-05-31       DAPQ24  ...         <NA>     0.0885
        3  2024-05-31       DAPU24  ...         <NA>     0.0865
        ...
    """
    selected_contract = str(contract_code).upper()
    converted_date = dc.convert_input_dates(date)

    # First, try to fetch historical data for the specified date
    df = fetch_historical_df(selected_contract, converted_date)

    bz_today = dt.datetime.now(TIMEZONE_BZ).date()
    # If there is no historical data available, try to fetch intraday data
    if converted_date.date() == bz_today and df.empty:
        df = fetch_intraday_df(selected_contract)

    return df


__all__ = ["futures", "ContractOptions"]
