import pandas as pd

from . import date_validator as dv
from . import futures as ft


def fetch_asset(
    asset_code: str, reference_date: str | pd.Timestamp | None = None
) -> pd.DataFrame:
    """
    Fetches data for a specified asset based on type and reference date.

    Args:
        asset_code (str): The asset code identifying the type of financial asset.
        Supported options:
            - "TRB": Brazilian treasury bonds (indicative rates from ANBIMA).
            - "LTN", "LFT", "NTN-F", "NTN-B": Specific types of Brazilian treasury bonds
                  (indicative rates from ANBIMA).
            - "DI1": One-day Interbank Deposit Futures (Futuro de DI) from B3.
            - "DDI": DI x U.S. Dollar Spread Futures (Futuro de Cupom Cambial) from B3.
            - "FRC": Forward Rate Agreement (FRA) from B3.
            - "DAP": DI x IPCA Spread Futures.
            - "DOL": U.S. Dollar Futures from B3.
            - "WDO": Mini U.S. Dollar Futures from B3.
            - "IND": Ibovespa Futures from B3.
            - "WIN": Mini Ibovespa Futures from B3.
        reference_date (str | pd.Timestamp | None): The reference date for which data is
            fetched. Defaults to the last business day if None. If the reference date is
            a string, it should be in 'DD-MM-YYYY' format.
        **kwargs: Additional keyword arguments, specifically:
            - return_raw (bool): Whether to return raw data without processing. Defaults
              to False.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched data for the specified asset.

    Raises:
        ValueError: If the asset code is not recognized or supported.

    Examples:
        >>> fetch_asset("LTN", "31-05-2024")
        >>> fetch_asset("DI1", "31-05-2024")
    """

    SUPPORTED_FUTURES = ["DI1", "DDI", "FRC", "DAP", "DOL", "WDO", "IND", "WIN"]

    normalized_date = dv.normalize_date(reference_date)

    today = pd.Timestamp.today().normalize()
    if normalized_date == today:
        return ft.fetch_intraday_df(future_code=asset_code.upper())

    if asset_code.upper() in SUPPORTED_FUTURES:
        return ft.fetch_historical_df(
            asset_code=asset_code.upper(), trade_date=normalized_date
        )

    raise ValueError("Asset type not supported.")
