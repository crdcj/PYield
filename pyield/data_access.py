import pandas as pd

from . import futures as ft
from . import indicators as it
from . import projections as pr
from . import treasuries as tr
from .utils import _normalize_date


def fetch_asset(
    asset_code: str,
    reference_date: str | pd.Timestamp | None = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Fetches data for a specified asset based on type and reference date.

    Args:
        asset_code (str): The asset code identifying the type of financial asset.
        Supported options:
            - "TRB": Treasury bonds (indicative rates from ANBIMA).
            - "LTN", "LFT", "NTN-F", "NTN-B": Specific types of Brazilian treasury bonds
                  (indicative rates from ANBIMA).
            - "DI1": One-day Interbank Deposit Futures (Futuro de DI) from B3.
            - "DDI": DI x U.S. Dollar Spread Futures (Futuro de Cupom Cambial) from B3.
        reference_date (str | pd.Timestamp | None): The reference date for which data is
            fetched. Defaults to the last business day if None.
        **kwargs: Additional keyword arguments, specifically:
            - return_raw (bool): Whether to return raw data without processing. Defaults
              to False.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched data for the specified asset.

    Raises:
        ValueError: If the asset code is not recognized or supported.

    Examples:
        >>> fetch_asset('TRB', '2023-04-01')
        >>> fetch_asset('DI1', '2023-04-01', return_raw=True)
    """
    return_raw = kwargs.get("return_raw", False)
    normalized_date = _normalize_date(reference_date)

    if asset_code.lower() == "trb":
        return tr.fetch_bonds(reference_date=normalized_date, return_raw=return_raw)
    elif asset_code.lower() in ["ltn", "lft", "ntn-f", "ntn-b"]:
        df = tr.fetch_bonds(reference_date=normalized_date)
        return df.query(f"BondType == '{asset_code.upper()}'")

    elif asset_code.lower() == "di1":
        today = pd.Timestamp.today().normalize()
        if normalized_date == today:
            return ft.fetch_last_di_data()
        else:
            return ft.fetch_past_di_data(
                trade_date=normalized_date, return_raw=return_raw
            )
    elif asset_code.lower() == "ddi":
        return ft.fetch_ddi(trade_date=normalized_date, return_raw=return_raw)
    else:
        raise ValueError("Asset type not supported.")


def fetch_indicator(
    indicator_code: str,
    reference_date: str | pd.Timestamp | None = None,
) -> float | None:
    """
    Fetches data for a specified economic indicator and reference date.

    Args:
        indicator_code (str): The code for the economic indicator. Supported options:
            - "SELIC": SELIC target rate from the Central Bank of Brazil.
            - "IPCA": IPCA monthly inflation rate from IBGE.
            - "DI": Interbank Deposit rate from B3.
            - "VNA_LFT": VNA (Valor Nominal Atualizado) of LFT (Tesouro Selic) bonds.
        reference_date (str | pd.Timestamp | None): The reference date for which data is
            fetched. Defaults to the last business day if None.

    Returns:
        pd.Series: A Series containing the fetched data for the specified indicator.

    Raises:
        ValueError: If the indicator code is not recognized or supported.

    Examples:
        >>> fetch_indicator('SELIC', '2023-04-01')
        >>> fetch_indicator('IPCA', '2023-04-01')
    """
    normalized_date = _normalize_date(reference_date)

    if indicator_code.lower() == "selic":
        return it.fetch_selic_target(reference_date=normalized_date)
    elif indicator_code.lower() == "ipca":
        return it.fetch_ipca_mr(reference_date=normalized_date)
    elif indicator_code.lower() == "di":
        return it.fetch_di(reference_date=normalized_date)
    elif indicator_code.lower() == "vna_lft":
        return it.fetch_vna_selic(reference_date=normalized_date)
    else:
        raise ValueError("Indicator type not supported.")


def fetch_projection(projection_code: str) -> dict:
    """
    Fetches a financial projection for a specified code and reference date.

    Args:
        projection_code (str): The code for the financial projection. Supported options:
            - "IPCA_CM": IPCA projection for the current month from ANBIMA.

    Returns:
        dict: A dictionary containing the fetched projection data.

    Raises:
        ValueError: If the projection code is not recognized or supported.

    Examples:
        >>> fetch_projection('IPCA_CM')
    """

    if projection_code.lower() == "ipca_cm":
        return pr.fetch_current_month_ipca_projection()
    else:
        raise ValueError("Projection type not supported.")
