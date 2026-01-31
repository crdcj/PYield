import datetime as dt

import polars as pl

from pyield.b3.futures.historical.historical_b3 import fetch_new_historical_df
from pyield.b3.futures.historical.historical_bmf import fetch_old_historical_df

API_CHANGE_DATE = dt.date(2025, 12, 12)


def fetch_historical_df(date: dt.date, contract_code: str) -> pl.DataFrame:
    """Fetches historical data for a specified futures contract and reference date."""
    if date > API_CHANGE_DATE:
        return fetch_new_historical_df(date, contract_code)
    else:
        # Try to fetch from old historical service
        return fetch_old_historical_df(date, contract_code)
