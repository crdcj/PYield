import pandas as pd

from ... import date_validator as dv
from .historical import fetch_historical_df
from .intraday import fetch_intraday_df

SUPPORTED_FUTURES = ["DI1", "DDI", "FRC", "DAP", "DOL", "WDO", "IND", "WIN"]


def futures(
    contract_code: str,
    reference_date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Fetches data for a specified futures contract based on type and reference date.

    Args:
        future_code (str): The futures contract code identifying the type of financial
        asset.
        Supported options:
            - "DI1": One-day Interbank Deposit Futures (Futuro de DI) from B3.
            - "DDI": DI x U.S. Dollar Spread Futures (Futuro de Cupom Cambial) from B3.
            - "FRC": Forward Rate Agreement (FRA) from B3.
            - "DAP": DI x IPCA Spread Futures.
            - "DOL": U.S. Dollar Futures from B3.
            - "WDO": Mini U.S. Dollar Futures from B3.
            - "IND": Ibovespa Futures from B3.
            - "WIN": Mini Ibovespa Futures from B3.
        reference_date (str | None): The reference date for which data is fetched.
            Defaults to the last business day if None. If the reference date is a
            string, it should be in 'DD-MM-YYYY' format.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched data for the specified futures
            contract.

    Raises:
        ValueError: If the futures contract code is not recognized or supported.

    Examples:
        >>> yd.futures.data("DI1", "31-05-2024")
        >>> yd.futures.data("DDI", "31-05-2024")
    """
    contract_code = contract_code.upper()
    if contract_code not in SUPPORTED_FUTURES:
        raise ValueError("Futures contract not supported.")

    normalized_date = dv.normalize_date(reference_date)

    today = pd.Timestamp.today().normalize()
    if normalized_date == today:
        df = fetch_intraday_df(contract_code)
    else:
        df = fetch_historical_df(contract_code, normalized_date)

    return df


__all__ = ["futures"]
