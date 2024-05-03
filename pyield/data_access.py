import pandas as pd

from . import bday
from . import futures as ft
from . import indicators as it
from . import projections as pr
from . import spreads as sp
from . import tn_bonds as tb


def _normalize_date(input_date: str | pd.Timestamp | None = None) -> pd.Timestamp:
    """
    Normalizes the given date to ensure it is a past business day at midnight. If no
    date is provided, it defaults to the last business day.

    Args:
        reference_date (str | pd.Timestamp | None): The date to normalize. Can be a
        string, pandas Timestamp or None. If None, it defaults to the last business day.

    Returns:
        pd.Timestamp: A normalized pandas Timestamp representing a past business day at
        midnight.

    Raises:
        ValueError: If the input date format is invalid, if the date is in the future,
        or if the date is not a business day.

    Notes:
        - Normalization means setting the time component of the timestamp to midnight.
        - The function checks if the normalized date is a business day and adjusts
          accordingly.
        - Business day calculations consider local market holidays.

    Examples:
        >>> _normalize_date('2023-04-01')
        >>> _normalize_date(pd.Timestamp('2023-04-01 15:30'))
        >>> _normalize_date()
    """
    if isinstance(input_date, str):
        # Convert string date to Timestamp and normalize to midnight
        normalized_date = pd.Timestamp(input_date).normalize()
    elif isinstance(input_date, pd.Timestamp):
        # Normalize Timestamp to midnight
        normalized_date = input_date.normalize()
    elif input_date is None:
        # If no date is provided, use the last available business day
        today = pd.Timestamp.today().normalize()
        normalized_date = bday.offset_bdays(dates=today, offset=0, roll="backward")
    else:
        raise ValueError(f"Date format not recognized: {input_date}")

    error_date = normalized_date.strftime("%d-%m-%Y")
    # Validate that the date is not in the future
    if normalized_date > pd.Timestamp.today().normalize():
        raise ValueError(f"Date {error_date} is in the future")
    # Validate that the date is a business day
    if not bday.is_bday(normalized_date):
        raise ValueError(f"Date {error_date} is not a business day")

    return normalized_date


def fetch_asset(
    asset_code: str, reference_date: str | pd.Timestamp | None = None
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
            - "FRC": Forward Rate Agreement (FRA) from B3.
            - "DAP": DI x IPCA Spread Futures.
            - "DOL": U.S. Dollar Futures from B3.
            - "WDO": Mini U.S. Dollar Futures from B3.
            - "IND": Ibovespa Futures from B3.
            - "WIN": Mini Ibovespa Futures from B3.
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
    SUPPORTED_BONDS = ["LTN", "LFT", "NTN-F", "NTN-B"]
    SUPPORTED_FUTURES = ["DI1", "DDI", "FRC", "DAP", "DOL", "WDO", "IND", "WIN"]

    normalized_date = _normalize_date(reference_date)

    today = pd.Timestamp.today().normalize()
    if normalized_date == today:
        return ft.fetch_intraday_df(future_code=asset_code.upper())

    if asset_code.upper() == "TRB":
        return tb.fetch_bonds(reference_date=normalized_date)

    if asset_code.upper() in SUPPORTED_BONDS:
        df = tb.fetch_bonds(reference_date=normalized_date)
        return df.query(f"BondType == '{asset_code.upper()}'")

    if asset_code.upper() in SUPPORTED_FUTURES:
        return ft.fetch_historical_df(
            asset_code=asset_code.upper(), trade_date=normalized_date
        )

    raise ValueError("Asset type not supported.")


def fetch_indicator(
    indicator_code: str,
    reference_date: str | pd.Timestamp | None = None,
) -> float | None:
    """
    Fetches data for a specified economic indicator and reference date.

    Args:
        indicator_code (str): The code for the economic indicator. Supported options:
            - "SELIC": SELIC target rate from the Central Bank of Brazil, expressed as
              an annual rate (p.a.).
            - "DI": Interbank Deposit rate (DI) from B3 expressed as a daily rate,
              calculated on a daily basis and expressed per diem (p.d.).
            - "IPCA": IPCA monthly inflation rate from IBGE, expressed per month (p.m.).
            - "VNA_LFT": VNA (Valor Nominal Atualizado) of LFT (Letra Financeira do
              Tesouro), which reflects updated nominal values for these bonds.
        - reference_date (str | pd.Timestamp | None): The reference date for which data
          is fetched. Defaults to the last business day if None.

    Returns:
        float | None: The value of the specified economic indicator for the reference
        date. Returns None if data is not found.

    Raises:
        ValueError: If the indicator code is not recognized or supported.

    Examples:
        >>> fetch_indicator('SELIC', '2023-04-01')
        0.1075  # Indicates a SELIC target rate of 10.75% p.a.
        >>> fetch_indicator('IPCA', '2023-03-10')
        0.0016  # Indicates an IPCA monthly rate of 0.16% p.m.
        >>> fetch_indicator('DI', '2023-04-17')
        0.00040168  # Indicates a DI daily rate of 0.02% p.d.
    """
    normalized_date = _normalize_date(reference_date)

    if indicator_code.upper() == "SELIC":
        return it.fetch_selic_target(reference_date=normalized_date)
    elif indicator_code.upper() == "IPCA":
        return it.fetch_ipca_mr(reference_date=normalized_date)
    elif indicator_code.upper() == "DI":
        return it.fetch_di(reference_date=normalized_date)
    elif indicator_code.upper() == "VNA_LFT":
        return it.fetch_vna_selic(reference_date=normalized_date)
    else:
        raise ValueError("Indicator type not supported.")


def fetch_projection(projection_code: str) -> pr.IndicatorProjection:
    """
    Fetches a financial projection for a specified code and reference date.

    Args:
        projection_code (str): The code for the financial projection. Supported options:
            - "IPCA_CM": IPCA projection for the current month from ANBIMA.

    Returns:
        IndicatorProjection: An instance of IndicatorProjection containing:
            - last_updated (pd.Timestamp): The datetime when the projection was last
              updated.
            - reference_month_ts (pd.Timestamp): The month to which the projection
              applies.
            - reference_month_br (str): The formatted month as a string formatted using
              the pt_BR locale.
            - projected_value (float): The projected indicator value.

    Raises:
        ValueError: If the projection code is not recognized or supported.

    Examples:
        >>> fetch_projection('IPCA_CM')
        IndicatorProjection(
            last_updated=pd.Timestamp('2024-04-19 18:55:00'),
            reference_month_ts=pd.Timestamp('2024-04-01 00:00:00'),
            reference_month_br='ABR/2024',
            projected_value=0.35
        )
    """

    if projection_code.upper() == "IPCA_CM":
        return pr.fetch_current_month_ipca_projection()
    else:
        raise ValueError("Projection type not supported.")


def calculate_spreads(
    spread_type: str, reference_date: str | pd.Timestamp | None = None
) -> pd.DataFrame:
    """
    Calculates spreads between assets based on the specified spread type.
    If no reference date is provided, the function uses the previous business day.

    Parameters:
        spread_type (str): The type of spread to calculate. Available options are:
            - "DI_vs_PRE": the spread between DI Futures and Treasury Pre-Fixed bonds.
        reference_date (str | pd.Timestamp, optional): The reference date for the
            spread calculation. If None or not provided, defaults to the previous
            business day according to the Brazilian calendar.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated spread in basis points.
        The data is sorted by asset type and maturity/expiration date.

    Raises:
        ValueError: If an invalid spread type is provided.
    """
    # Normalize the reference date
    normalized_date = _normalize_date(reference_date)
    if spread_type.upper() == "DI_VS_PRE":
        return sp.calculate_di_spreads(normalized_date)
    else:
        raise ValueError("Invalid spread type.")
