"""
This module provides functions to fetch various financial indicators from
the Brazilian Central Bank's API using Polars.

Implementation Notes:
    - Values are retrieved in percentage format and converted to decimal format
      (divided by 100).
    - Each rate type is rounded to maintain the same precision as provided by
      the Central Bank:
        - SELIC Over and SELIC Target: 4 decimal places
        - DI Over: 8 decimal places for daily rates. For annualized rates,
          the value is rounded to 4 decimal places.
    - For requests spanning more than 10 years, the date range is automatically
      chunked using Polars' native calendar-aware date functionalities.
"""

import logging
from enum import Enum
from typing import Any

import polars as pl
import requests

from pyield import clock
from pyield.converters import convert_dates
from pyield.retry import default_retry
from pyield.types import DateLike, has_nullable_args

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
DECIMAL_PLACES_ANNUALIZED = 4  # 2 decimal places in percentage format
DECIMAL_PLACES_DAILY = 8  # 6 decimal places in percentage format

# 404 Not Found error code for resource not found in the API
ERROR_CODE_NOT_FOUND = 404

# 400 Bad Request error code for invalid requests
ERROR_CODE_BAD_REQUEST = 400

# Limite de segurança em dias, correspondendo a ~9.5 anos.
# Evita a complexidade do cálculo exato de 10 anos-calendário.
SAFE_DAYS_THRESHOLD = 3500  # aprox 365 * 9.5


class BCSerie(Enum):
    """Enum para as séries disponíveis no Banco Central."""

    SELIC_OVER = 1178
    SELIC_TARGET = 432
    DI_OVER = 11


@default_retry
def _do_api_call(api_url: str) -> list[dict[str, Any]]:
    """Executa uma chamada GET na API do BCB e retorna o JSON."""
    response = requests.get(api_url, timeout=10)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


def _build_download_url(
    serie: BCSerie, start: DateLike, end: DateLike | None = None
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
    start = convert_dates(start)
    start_str = start.strftime("%d/%m/%Y")

    api_url = BASE_URL
    api_url += f"{serie.value}/dados?formato=json"
    api_url += f"&dataInicial={start_str}"

    if end:
        end = convert_dates(end)
        end_str = end.strftime("%d/%m/%Y")
        api_url += f"&dataFinal={end_str}"

    return api_url


def _fetch_request(
    serie: BCSerie,
    start: DateLike,
    end: DateLike | None,
) -> pl.DataFrame:
    """
    Worker function that fetches data from the API.
    """
    # Define o esquema esperado para o DataFrame de retorno.
    # Isso é crucial para os casos em que a API não retorna dados.
    expected_schema = {"Date": pl.Date, "Value": pl.Float64}

    api_url = _build_download_url(serie, start, end)

    try:
        data = _do_api_call(api_url)
        if not data:
            logger.warning(f"No data available for the requested period: {api_url}")
            return pl.DataFrame(schema=expected_schema)

        df = (
            pl.from_dicts(data)
            .with_columns(
                Date=pl.col("data").str.to_date("%d/%m/%Y"),
                Value=pl.col("valor").cast(pl.Float64) / 100,
            )
            .select("Date", "Value")
        )
        return df

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:  # noqa
            logger.warning(
                f"Resource not found (404), treated as no data: {e.request.url}"
            )
            return pl.DataFrame(schema=expected_schema)

        # Qualquer outro erro HTTP final para o programa.
        raise


def _fetch_data_from_url(
    serie: BCSerie, start: DateLike, end: DateLike | None = None
) -> pl.DataFrame:
    """
    Orchestrates fetching data from the Central Bank API, handling requests longer
    than 10 years by splitting them into smaller chunks using polars date_range.

    Args:
        serie: The series enum to fetch
        start: The start date for the data to fetch
        end: The end date for the data

    Returns:
        DataFrame with the requested data
    """
    # 1. Converter datas usando a função auxiliar existente
    start_date = convert_dates(start)
    # Se a data final não for fornecida, usar a data de hoje para o cálculo do período
    end_date = convert_dates(end) if end else clock.today()

    # Verificação simples e pragmática baseada em dias. Se o período for
    # menor que nosso limite de segurança, faz uma chamada única.
    if (end_date - start_date).days < SAFE_DAYS_THRESHOLD:
        return _fetch_request(serie, start_date, end_date)

    # 3. Se for maior, quebrar em pedaços (chunking)
    logger.info("Date range exceeds 10 years. Fetching data in chunks.")

    duration_str = "10y"

    chunk_starts = pl.date_range(
        start=start_date, end=end_date, interval=duration_str, eager=True
    )

    chunk_ends = chunk_starts.dt.offset_by(duration_str)

    chunks_df = pl.DataFrame({"start": chunk_starts, "end": chunk_ends}).with_columns(
        pl.when(pl.col("end") > end_date)
        .then(pl.lit(end_date))
        .otherwise(pl.col("end"))
        .alias("end")
    )

    all_dfs = [
        _fetch_request(serie, chunk["start"], chunk["end"])
        for chunk in chunks_df.iter_rows(named=True)
    ]

    all_dfs = [df for df in all_dfs if not df.is_empty()]

    if not all_dfs:
        return pl.DataFrame()

    return pl.concat(all_dfs).unique(subset=["Date"], keep="first").sort("Date")


def selic_over_series(
    start: DateLike,
    end: DateLike | None = None,
) -> pl.DataFrame:
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
        >>> # No data on 26-01-2025 (sunday). Selic changed due to Copom meeting.
        >>> bc.selic_over_series("26-01-2025").head(5)  # Showing first 5 rows
        shape: (5, 2)
        ┌────────────┬────────┐
        │ Date       ┆ Value  │
        │ ---        ┆ ---    │
        │ date       ┆ f64    │
        ╞════════════╪════════╡
        │ 2025-01-27 ┆ 0.1215 │
        │ 2025-01-28 ┆ 0.1215 │
        │ 2025-01-29 ┆ 0.1215 │
        │ 2025-01-30 ┆ 0.1315 │
        │ 2025-01-31 ┆ 0.1315 │
        └────────────┴────────┘

        >>> # Fetching data for a specific date range
        >>> bc.selic_over_series("14-09-2025", "17-09-2025")
        shape: (3, 2)
        ┌────────────┬───────┐
        │ Date       ┆ Value │
        │ ---        ┆ ---   │
        │ date       ┆ f64   │
        ╞════════════╪═══════╡
        │ 2025-09-15 ┆ 0.149 │
        │ 2025-09-16 ┆ 0.149 │
        │ 2025-09-17 ┆ 0.149 │
        └────────────┴───────┘
    """
    if has_nullable_args(start):  # Start must be provided
        return pl.DataFrame()
    df = _fetch_data_from_url(BCSerie.SELIC_OVER, start, end)
    return df.with_columns(pl.col("Value").round(DECIMAL_PLACES_ANNUALIZED))


def selic_over(date: DateLike) -> float:
    """
    Fetches the SELIC Over rate value for a specific date.

    This is a convenience function that returns only the value (not the DataFrame)
    for the specified date.

    Args:
        date: The reference date to fetch the SELIC Over rate for.

    Returns:
        The SELIC Over rate as a float or None if not available.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_over("31-05-2024")
        0.104
    """
    if has_nullable_args(date):
        return float("nan")
    df = selic_over_series(date, date)
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)


def selic_target_series(
    start: DateLike,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """
    Fetches the SELIC Target rate from the Brazilian Central Bank.

    The SELIC Target rate is the official interest rate set by the
    Central Bank of Brazil's Monetary Policy Committee (COPOM).

    API URL Example:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: The start date for the data to fetch.
        end: The end date for the data to fetch. If None, returns data up to
             the latest available date.

    Returns:
        DataFrame containing Date and Value columns with the SELIC Target rate,
        or empty DataFrame if data is not available

    Examples:
        >>> from pyield import bc
        >>> bc.selic_target_series("31-05-2024", "31-05-2024")
        shape: (1, 2)
        ┌────────────┬───────┐
        │ Date       ┆ Value │
        │ ---        ┆ ---   │
        │ date       ┆ f64   │
        ╞════════════╪═══════╡
        │ 2024-05-31 ┆ 0.105 │
        └────────────┴───────┘
    """
    if has_nullable_args(start):  # Start must be provided
        return pl.DataFrame()
    df = _fetch_data_from_url(BCSerie.SELIC_TARGET, start, end)
    df = df.with_columns(pl.col("Value").round(DECIMAL_PLACES_ANNUALIZED))
    return df


def selic_target(date: DateLike) -> float:
    """
    Fetches the SELIC Target rate value for a specific date.

    This is a convenience function that returns only the value (not the DataFrame)
    for the specified date.

    Args:
        date: The reference date to fetch the SELIC Target rate for.

    Returns:
        The SELIC Target rate as a float or None if not available.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_target("31-05-2024")
        0.105
    """
    if has_nullable_args(date):
        return float("nan")
    df = selic_target_series(date, date)
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)


def di_over_series(
    start: DateLike,
    end: DateLike | None = None,
    annualized: bool = True,
) -> pl.DataFrame:
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
        >>> # Returns all data since 29-01-2025
        >>> bc.di_over_series("29-01-2025").head(5)  # Showing only first 5 rows
        shape: (5, 2)
        ┌────────────┬────────┐
        │ Date       ┆ Value  │
        │ ---        ┆ ---    │
        │ date       ┆ f64    │
        ╞════════════╪════════╡
        │ 2025-01-29 ┆ 0.1215 │
        │ 2025-01-30 ┆ 0.1315 │
        │ 2025-01-31 ┆ 0.1315 │
        │ 2025-02-03 ┆ 0.1315 │
        │ 2025-02-04 ┆ 0.1315 │
        └────────────┴────────┘
    """
    if has_nullable_args(start):
        return pl.DataFrame()
    df = _fetch_data_from_url(BCSerie.DI_OVER, start, end)
    if annualized:
        df = df.with_columns(
            (((pl.col("Value") + 1).pow(252)) - 1)
            .round(DECIMAL_PLACES_ANNUALIZED)
            .alias("Value")
        )

    else:
        df = df.with_columns(pl.col("Value").round(DECIMAL_PLACES_DAILY))

    return df


def di_over(date: DateLike, annualized: bool = True) -> float:
    """
    Fetches the DI Over rate value for a specific date.

    This is a convenience function that returns only the value (not the DataFrame)
    for the specified date.

    Args:
        date: The reference date to fetch the DI Over rate for.
        annualized: If True, returns the annualized rate (252 trading
            days per year), otherwise returns the daily rate.

    Returns:
        The DI Over rate as a float or float("nan") if not available.

    Examples:
        >>> from pyield import bc
        >>> bc.di_over("31-05-2024")
        0.104

        >>> bc.di_over("28-01-2025", annualized=False)
        0.00045513
    """
    if has_nullable_args(date):
        return float("nan")
    df = di_over_series(date, date, annualized)
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)
