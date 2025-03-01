import logging
from enum import Enum
from functools import lru_cache
from urllib.error import HTTPError

import pandas as pd

from pyield.config import default_retry
from pyield.date_converter import DateScalar, convert_input_dates

logger = logging.getLogger(__name__)
BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."


class BCSerie(Enum):
    """Enum para as séries disponíveis no Banco Central."""

    SELIC_OVER = 1178
    SELIC_TARGET = 432
    DI_OVER = 11


def _build_download_url(serie: BCSerie, date: DateScalar) -> str:
    """Constrói a URL para download dos dados da série do Banco Central."""
    date = convert_input_dates(date)
    formatted_date = date.strftime("%d/%m/%Y")

    api_url = BASE_URL
    api_url += f"{serie.value}/dados?formato=csv"
    api_url += f"&dataInicial={formatted_date}&dataFinal={formatted_date}"

    return api_url


@default_retry
def _fetch_data_from_url(date: DateScalar, serie: BCSerie) -> pd.DataFrame:
    """Busca os dados da série do Banco Central a partir da URL."""
    api_url = _build_download_url(BCSerie.SELIC_OVER, date)
    try:
        df = pd.read_csv(api_url, sep=";", decimal=",", dtype_backend="numpy_nullable")
        if df.empty or "valor" not in df.columns:
            raise ValueError(f"No data available for {serie.name} on {date}")

        df = df.rename(columns={"data": "Date", "valor": "Value"})
        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
        df["Value"] = (df["Value"] / 100).round(4)
        return df
    except HTTPError as e:
        if e.code == 404:  # noqa
            # Tratamento específico para erro 404
            logger.warning(f"Recurso não encontrado (404): {api_url}")
            # Retorna DataFrame vazio em vez de lançar exceção para erro 404
            return pd.DataFrame()
        else:
            logger.error(f"Erro HTTP ao acessar a API do BC: {e}")
            raise


@lru_cache(maxsize=128)
def selic_over(date: DateScalar) -> pd.DataFrame:
    """
    Fetches the SELIC Over rate for a specific reference date.

    The SELIC Over rate is the daily average interest rate effectively practiced
    between banks in the interbank market, using public securities as collateral.

    Args:
        date (pd.Timestamp): The date for which to fetch the SELIC Over rate.

    Returns:
        float: The SELIC Over rate as a float rounded to 4 decimal places or NaN if
        the rate is not available.

    https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Examples:
        >>> bc.selic_over("31-05-2024")
        0.104
    """
    df = _fetch_data_from_url(date, BCSerie.SELIC_OVER)
    return df


def selic_target(date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetches the SELIC Target rate for a specific reference date.

    The SELIC Target rate is the official rate set by the Central Bank of Brazil.

    Args:
        date (pd.Timestamp): The date for which to fetch the SELIC Target.

    Returns:
        float: The SELIC Target rate as a float rounded to 4 decimal places or NaN if
        the rate is not available.

    Examples:
        >>> bc.selic_target()"31-05-2024")
        0.105

    https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    """
    return _fetch_data_from_url(date, BCSerie.SELIC_TARGET)


def di_over(date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetches the DI (Interbank Deposit) rate for a specific reference date.

    Example:
        >>> bc.di_over("31-05-2024")
        0.104

    https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    """
    return _fetch_data_from_url(date, BCSerie.DI_OVER)
