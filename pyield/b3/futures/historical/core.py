import datetime as dt

import polars as pl

from pyield.b3.futures.historical.historical_new import fetch_new_historical_df
from pyield.b3.futures.historical.historical_old import fetch_old_historical_df


def fetch_historical_df(date: dt.date, contract_code: str) -> pl.DataFrame:
    """Fetches historical data for a specified futures contract and reference date."""
    # If is before 2025-12-12, try to fetch from BMF legacy service first
    if date <= dt.date(2025, 12, 12):
        df = fetch_old_historical_df(date, contract_code)
        if not df.is_empty():  # If data is found from Historical A
            return df

    # Try to fetch from new historical service
    return fetch_new_historical_df(date, contract_code)
