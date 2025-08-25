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
import requests

from pyield.date_converter import DateScalar, convert_input_dates
from pyield.retry import default_retry

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
DECIMAL_PLACES_ANNUALIZED = 4  # 2 decimal places in percentage format
DECIMAL_PLACES_DAILY = 8  # 6 decimal places in percentage format

# 404 Not Found error code for resource not found in the API
ERROR_CODE_NOT_FOUND = 404

# 400 Bad Request error code for invalid requests
ERROR_CODE_BAD_REQUEST = 400


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
    api_url += f"{serie.value}/dados?formato=json"
    api_url += f"&dataInicial={start_str}&dataFinal={end_str}"

    return api_url


@default_retry
def _fetch_request(
    serie: BCSerie,
    start: DateScalar | None,
    end: DateScalar | None,
) -> pd.DataFrame:
    """
    Worker function that fetches data from the API.
    """
    api_url = _build_download_url(serie, start, end)
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data:
            logger.warning(f"No data available for the requested period: {api_url}")
            return pd.DataFrame()

        df = pd.DataFrame(data, dtype="string")
        df = df.rename(columns={"data": "Date", "valor": "Value"})
        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y").astype(
            "date32[pyarrow]"
        )
        df["Value"] = (df["Value"].astype("float64[pyarrow]")) / 100

        return df

    except HTTPError as e:
        if e.response.status_code == ERROR_CODE_NOT_FOUND:
            logger.warning(f"Resource not found (404): {api_url}")
            return pd.DataFrame()
        if (
            e.response.status_code == ERROR_CODE_BAD_REQUEST
            and "janela de consulta" in e.response.text
        ):
            logger.error(
                "API request exceeded the 10-year limit. "
                "This should have been handled by the chunking logic. URL: %s",
                api_url,
            )
        else:
            logger.error(f"HTTP error accessing Central Bank API: {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching data from Central Bank API: {e}")
        raise


def _fetch_data_from_url(
    serie: BCSerie, start: DateScalar | None = None, end: DateScalar | None = None
) -> pd.DataFrame:
    """
    Orchestrates fetching data from the Central Bank API, handling requests longer
    than 10 years by splitting them into smaller chunks using pandas DateOffset.

    Args:
        serie: The series enum to fetch
        start: The start date for the data to fetch
        end: The end date for the data

    Returns:
        DataFrame with the requested data
    """
    # 1. Converter datas usando a função auxiliar existente e o pandas
    start_date = convert_input_dates(start) if start else None
    # Se a data final não for fornecida, usar a data de hoje para o cálculo do período
    end_date = convert_input_dates(end) if end else pd.to_datetime("today").normalize()

    # Se não houver data de início, a API já limita a 10 anos. Chamada direta é segura.
    if not start_date:
        return _fetch_request(serie, start, end)

    # 2. Verificar se o período é maior que 10 anos usando pd.DateOffset
    if end_date < start_date + pd.DateOffset(years=10):
        return _fetch_request(serie, start_date, end_date)

    # 3. Se for maior, quebrar em pedaços (chunking)
    logger.info("Date range exceeds 10 years. Fetching data in chunks.")
    all_dfs = []
    current_start = start_date

    while current_start < end_date:
        # Fim do chunk é 10 anos à frente, menos 1 dia.
        chunk_end = current_start + pd.DateOffset(years=10) - pd.DateOffset(days=1)

        # Garantir que o fim do chunk não ultrapasse a data final solicitada
        chunk_end = min(chunk_end, end_date)

        # Buscar os dados para este chunk
        df_chunk = _fetch_request(serie, current_start, chunk_end)
        if not df_chunk.empty:
            all_dfs.append(df_chunk)

        # Preparar o início do próximo chunk
        current_start = chunk_end + pd.DateOffset(days=1)

    if not all_dfs:
        return pd.DataFrame()

    # 4. Concatenar todos os DataFrames e retornar o resultado
    final_df = pd.concat(all_dfs, ignore_index=True)
    return final_df


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
    if df.empty:
        return float("nan")
    return round(df.at[0, "Value"], 4)


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
    if df.empty:
        return float("nan")
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
    if df.empty:
        return float("nan")
    return float(df.at[0, "Value"])
