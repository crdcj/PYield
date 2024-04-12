import pandas as pd

from . import di
from . import treasuries as tr
from .utils import _normalize_date


def get_data(
    asset_code: str,
    reference_date: str | pd.Timestamp | None = None,
    return_raw: bool = False,
) -> pd.DataFrame:
    """
    Fetches data for a specified asset type and reference date.

    Args:
    asset (str): The asset type. Available options are:
        - "TRE": Fetches indicative treasury rates from ANBIMA.
        - "DI1": Fetches DI Futures rates from B3.
    reference_date (str): Reference date for the data in YYYY-MM-DD format.
    return_raw (bool): If true, returns raw data without processing. Defaults to False.

    Returns:
    pd.DataFrame: A DataFrame containing the fetched data.

    Raises:
    ValueError: If the specified source or asset type is not supported.
    """
    # Validate the date, defaulting to the previous business day if not provided
    normalized_date = _normalize_date(reference_date)

    if asset_code.lower() == "tre":
        # Fetch all indicative treasury rates from ANBIMA
        return tr.fetch_data(reference_date=normalized_date, return_raw=return_raw)
    elif asset_code.lower() == "di1":
        return di.fetch_data(
            trade_date=normalized_date, source_type="bmf", return_raw=return_raw
        )
    else:
        raise ValueError("Asset type not supported.")
