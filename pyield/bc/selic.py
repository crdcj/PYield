import logging

import pandas as pd

from pyield.config import global_retry
from pyield.date_converter import DateScalar, convert_input_dates

logger = logging.getLogger(__name__)


def _build_download_url(date: DateScalar) -> str:
    date = convert_input_dates(date)
    fdate = date.strftime("%d/%m/%Y")
    # https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    api_url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=csv&dataInicial={fdate}&dataFinal={fdate}"
    return api_url


@global_retry
def _fetch_data_from_url(file_url: str) -> pd.DataFrame:
    return pd.read_csv(file_url, sep=";", decimal=",")


def _process_selic_over_df(df: pd.DataFrame) -> float:
    if df.empty or "valor" not in df.columns:
        msg = "No data available for SELIC Over rate"
        logger.warning(msg)
        return float("nan")
    value = float(df["valor"].iloc[0] / 100)  # SELIC Over daily rate
    return round(value, 4)


def selic_over(date: DateScalar) -> float:
    """
    Fetches the SELIC Over rate for a specific reference date.

    The SELIC Over rate is the daily average interest rate effectively practiced
    between banks in the interbank market, using public securities as collateral.

    Args:
        date (pd.Timestamp): The date for which to fetch the SELIC Over rate.

    Returns:
        float: The SELIC Over rate as a float rounded to 4 decimal places or NaN if
        the rate is not available.

    Examples:
        >>> yd.indicator("31-05-2024")
        0.104

    """
    api_url = _build_download_url(date)
    df = _fetch_data_from_url(api_url)
    return _process_selic_over_df(df)


def selic_target(date: pd.Timestamp) -> float:
    """
    Fetches the SELIC Target rate for a specific reference date.

    The SELIC Target rate is the official rate set by the Central Bank of Brazil.

    Args:
        date (pd.Timestamp): The date for which to fetch the SELIC Target.

    Returns:
        float: The SELIC Target rate as a float rounded to 4 decimal places or NaN if
        the rate is not available.

    Examples:
        >>> yd.indicator("31-05-2024")
        0.105
    """
    # https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    date_str = date.strftime("%d/%m/%Y")
    api_url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=csv&dataInicial={date_str}&dataFinal={date_str}"
    df = pd.read_csv(api_url, sep=";", decimal=",")
    if df.empty or "valor" not in df.columns:
        msg = f"No data available for SELIC Target rate on {date}"
        logger.warning(msg)
        return float("nan")
    value = float(df["valor"].iloc[0] / 100)  # SELIC Target rate
    return round(float(value), 4)


def di_over(date: pd.Timestamp) -> float:
    """
    Fetches the DI (Interbank Deposit) rate for a specific reference date.

    Example:
        >>> yd.indicator("31-05-2024")
        0.104
    """

    # https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    di_date = date.strftime("%d/%m/%Y")
    api_url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=csv&dataInicial={di_date}&dataFinal={di_date}"
    df = pd.read_csv(api_url, sep=";", decimal=",")
    if df.empty or "valor" not in df.columns:
        logger.warning(f"No data available for DI rate on {date}")
        return float("nan")
    value = float(df["valor"].iloc[0] / 100)  # DI daily rate
    return round((1 + value) ** 252 - 1, 4)  # Annualize the daily rate
