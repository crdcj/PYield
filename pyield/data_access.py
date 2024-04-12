import pandas as pd

from . import di
from . import treasuries as tr
from .utils import _normalize_date


def get_data(
    asset: str,
    reference_date: str | pd.Timestamp | None = None,
    return_raw: bool = False,
) -> pd.DataFrame:
    """
    Fetches data for a specified asset from a given reference date.

    Args:
    asset (str): The asset type (e.g., 'treasuries', 'DI Futures').
    reference_date (str): Reference date for the data in YYYY-MM-DD format.
    return_raw (bool): If true, returns raw data without processing. Defaults to False.

    Returns:
    pd.DataFrame: A DataFrame containing the fetched data.

    Raises:
    ValueError: If the specified source or asset type is not supported.
    """
    # Validate the date, defaulting to the previous business day if not provided
    normalized_date = _normalize_date(reference_date)

    if asset.lower() == "treasuries":
        # Fetch all indicative treasury rates from ANBIMA
        return tr.fetch_data(reference_date=normalized_date, return_raw=return_raw)
    elif asset.lower() == "di futures":
        return di.fetch_data(
            trade_date=normalized_date, source_type="bmf", return_raw=return_raw
        )
    else:
        raise ValueError("Asset type not supported.")
