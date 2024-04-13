import pandas as pd

from . import di
from . import treasuries as tr
from .utils import _normalize_date


def get_data(
    asset_code: str,
    reference_date: str | pd.Timestamp | None = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Fetches data for a specified asset type and reference date.

    Args:
    asset (str): The asset type. Available options are:
        - "TRB": Fetches indicative rates for Brazilian treasury bonds from ANBIMA.
        - "LTN": Fetches indicative rates for Brazilian LTN bonds from ANBIMA.
        - "LFT": Fetches indicative rates for Brazilian LFT bonds from ANBIMA.
        - "NTN-F": Fetches indicative rates for Brazilian NTN-F bonds from ANBIMA.
        - "NTN-B": Fetches indicative rates for Brazilian NTN-B bonds from ANBIMA.
        - "DI1": Fetches DI Futures rates from B3.
    reference_date (str): Reference date for the data in YYYY-MM-DD format.

    Returns:
    pd.DataFrame: A DataFrame containing the fetched data.

    Raises:
    ValueError: If the specified source or asset type is not supported.
    """
    # Extract the internal use parameter with default value
    return_raw = kwargs.get("return_raw", False)

    # Validate the date, defaulting to the previous business day if not provided
    normalized_date = _normalize_date(reference_date)

    if asset_code.lower() == "trb":
        # Fetch all indicative treasury rates from ANBIMA
        return tr.fetch_data(reference_date=normalized_date, return_raw=return_raw)
    elif asset_code.lower() in ["ltn", "lft", "ntn-f", "ntn-b"]:
        # Fetch indicative rates for a specific type of Brazilian treasury bond
        df = tr.fetch_data(reference_date=normalized_date)
        return df.query(f"BondType == '{asset_code.upper()}'")

    elif asset_code.lower() == "di1":
        return di.fetch_data(
            trade_date=normalized_date, source_type="bmf", return_raw=return_raw
        )
    else:
        raise ValueError("Asset type not supported.")
