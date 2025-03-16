"""
This module provides functions to fetch various financial indicators from
the Brazilian Central Bank's API.

Implementation Notes:
    - Values are retrieved in percentage format and converted to decimal format
      (divided by 100).
    - Each rate type is rounded to maintain the same precision as provided by
      the Central Bank:
        - SELIC Over and SELIC Target: 4 decimal places (from 2 decimal places
          in percentage format)
        - DI Over: 8 decimal places for daily rates (from 6 decimal places in % format).
          For annualized rates, the value is rounded to 4 decimal places.

"""

import logging
from enum import Enum
from urllib.error import HTTPError

import pandas as pd

from pyield.date_converter import DateScalar, convert_input_dates
from pyield.retry import default_retry

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
DECIMAL_PLACES_ANNUALIZED = 4  # 2 decimal places in percentage format
DECIMAL_PLACES_DAILY = 8  # 6 decimal places in percentage format


class BCSerie(Enum):
    """Enum para as séries disponíveis no Banco Central."""

    SELIC_OVER = 1178
    SELIC_TARGET = 432
    DI_OVER = 11


def _build_download_url(
    serie: BCSerie, start: DateScalar | None = None, end: DateScalar | None = None
) -> str:
    """
    Builds the URL for downloading data from the Central Bank series.

    Args:
        serie: The series enum value to fetch
        start: The start date for the data to fetch
        end: The end date for the data

    Returns:
        The formatted URL for the API request
    """
    start_str = ""
    if start:
        start = convert_input_dates(start)
        start_str = start.strftime("%d/%m/%Y")

    end_str = ""
    if end:
        end = convert_input_dates(end)
        end_str = end.strftime("%d/%m/%Y")

    api_url = BASE_URL
    api_url += f"{serie.value}/dados?formato=csv"
    api_url += f"&dataInicial={start_str}&dataFinal={end_str}"

    return api_url


@default_retry
def _fetch_data_from_url(
    serie: BCSerie, start: DateScalar | None = None, end: DateScalar | None = None
) -> pd.DataFrame:
    """
    Fetches data from the Central Bank API with retry mechanism.

    Args:
        serie: The series enum to fetch
        start: The start date for the data to fetch
        end: The end date for the data

    Returns:
        DataFrame with the requested data

    Raises:
        HTTPError: If the resource is not found (404) or other HTTP error
        Various exceptions: For other errors after retry attempts are exhausted
    """
    api_url = _build_download_url(serie, start, end)

    try:
        df = pd.read_csv(api_url, sep=";", decimal=",", dtype_backend="numpy_nullable")
        if df.empty or "valor" not in df.columns:
            logger.warning(f"No data available for the requested period: {api_url}")
            return pd.DataFrame()

        # Process the dataframe
        df = df.rename(columns={"data": "Date", "valor": "Value"})
        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
        # Value is in percentage format, so divide by 100
        df["Value"] /= 100
        return df
    except HTTPError as e:
        if e.code == 404:  # noqa
            logger.warning(f"Resource not found (404): {api_url}")
            return pd.DataFrame()
        else:
            logger.error(f"HTTP error accessing Central Bank API: {e}")
            raise
    except pd.errors.ParserError as e:
        # For parsing errors, log and re-raise to allow retry
        logger.warning(f"CSV parsing error (possibly HTML or invalid format): {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching data from Central Bank API: {e}")
        raise


def selic_over_series(
    start: DateScalar | None = None, end: DateScalar | None = None
) -> pd.DataFrame:
    """
    Fetches the SELIC Over rate from the Brazilian Central Bank.

    The SELIC Over rate is the daily average interest rate effectively practiced
    between banks in the interbank market, using public securities as collateral.

    API URL Example:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: The start date for the data to fetch. If None, returns data from
              the earliest available date.
        end: The end date for the data to fetch. If None, returns data up to
             the latest available date.

    Returns:
        DataFrame containing Date and Value columns with the SELIC Over rate,
        or empty DataFrame if data is not available.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_over_series("31-05-2024", "31-05-2024")
                Date  Value
        0 2024-05-31  0.104

        >>> # No data on 26-01-2025 (sunday). Rate changed due to Copom meeting.
        >>> bc.selic_over_series("26-01-2025")  # Returns all data since 26-01-2025
                Date   Value
        0 2025-01-27  0.1215
        1 2025-01-28  0.1215
        2 2025-01-29  0.1215
        3 2025-01-30  0.1315
        4 2025-01-31  0.1315
        ...

    """
    df = _fetch_data_from_url(BCSerie.SELIC_OVER, start, end)
    df["Value"] = df["Value"].round(DECIMAL_PLACES_ANNUALIZED)
    return df


def selic_over(date: DateScalar) -> float:
    """
    Fetches the SELIC Over rate value for a specific date.

    This is a convenience function that returns only the value (not the DataFrame)
    for the specified date.

    Args:
        date: The reference date to fetch the SELIC Over rate for.

    Returns:
        The SELIC Over rate as a float.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_over("31-05-2024")
        0.104
    """
    df = selic_over_series(date, date)
    return float(df.at[0, "Value"])


def selic_target_series(
    start: DateScalar | None = None, end: DateScalar | None = None
) -> pd.DataFrame:
    """
    Fetches the SELIC Target rate from the Brazilian Central Bank.

    The SELIC Target rate is the official interest rate set by the
    Central Bank of Brazil's Monetary Policy Committee (COPOM).

    API URL Example:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: The start date for the data to fetch. If None, returns data from
              the earliest available date.
        end: The end date for the data to fetch. If None, returns data up to
             the latest available date.

    Returns:
        DataFrame containing Date and Value columns with the SELIC Target rate,
        or empty DataFrame if data is not available

    Examples:
        >>> from pyield import bc
        >>> bc.selic_target_series("31-05-2024", "31-05-2024")
                Date  Value
        0 2024-05-31  0.105
    """
    df = _fetch_data_from_url(BCSerie.SELIC_TARGET, start, end)
    df["Value"] = df["Value"].round(DECIMAL_PLACES_ANNUALIZED)
    return df


def selic_target(date: DateScalar) -> float:
    """
    Fetches the SELIC Target rate value for a specific date.

    This is a convenience function that returns only the value (not the DataFrame)
    for the specified date.

    Args:
        date: The reference date to fetch the SELIC Target rate for.

    Returns:
        The SELIC Target rate as a float.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_target("31-05-2024")
        0.105
    """
    df = selic_target_series(date, date)
    return float(df.at[0, "Value"])


def di_over_series(
    start: DateScalar | None = None,
    end: DateScalar | None = None,
    annualized: bool = True,
) -> pd.DataFrame:
    """
    Fetches the DI (Interbank Deposit) rate from the Brazilian Central Bank.

    The DI rate represents the average interest rate of interbank loans.

    API URL Example:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=csv&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: The start date for the data to fetch. If None, returns data from
              the earliest available date.
        end: The end date for the data to fetch. If None, returns data up to
             the latest available date.
        annualized: If True, returns the annualized rate (252 trading
            days per year), otherwise returns the daily rate.

    Returns:
        DataFrame containing Date and Value columns with the DI rate,
        or empty DataFrame if data is not available.

    Examples:
        >>> from pyield import bc
        >>> bc.di_over_series("29-01-2025")
                Date   Value
        0  2025-01-29  0.1215
        1  2025-01-30  0.1315
        2  2025-01-31  0.1315
        3  2025-02-03  0.1315
        ...

    """
    df = _fetch_data_from_url(BCSerie.DI_OVER, start, end)
    if annualized:
        df["Value"] = (df["Value"] + 1) ** 252 - 1
        df["Value"] = df["Value"].round(DECIMAL_PLACES_ANNUALIZED)
    else:
        df["Value"] = df["Value"].round(DECIMAL_PLACES_DAILY)
    return df


def di_over(date: DateScalar, annualized: bool = True) -> float:
    """
    Fetches the DI Over rate value for a specific date.

    This is a convenience function that returns only the value (not the DataFrame)
    for the specified date.

    Args:
        date: The reference date to fetch the DI Over rate for.
        annualized: If True, returns the annualized rate (252 trading
            days per year), otherwise returns the daily rate.

    Returns:
        The DI Over rate as a float.

    Examples:
        >>> from pyield import bc
        >>> bc.di_over("31-05-2024")
        0.104

        >>> bc.di_over("28-01-2025", annualized=False)
        0.00045513
    """
    df = di_over_series(date, date, annualized)
    return float(df.at[0, "Value"])
