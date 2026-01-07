import logging

import polars as pl
import requests

from pyield import clock

logger = logging.getLogger(__name__)

API_BASE_URL = (
    "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/benchmarks"
)

API_HISTORY_PARAM = "incluir_historico"


COLUMN_MAPPING = {
    "INÍCIO": "StartDate",
    "TERMINO": "EndDate",
    "TÍTULO": "BondType",
    "VENCIMENTO": "MaturityDate",
    "BENCHMARK": "Benchmark",
}

DATA_SCHEMA = {
    "BondType": pl.String,
    "MaturityDate": pl.Date,
    "Benchmark": pl.String,
    "StartDate": pl.Date,
    "EndDate": pl.Date,
}

FINAL_COLUMN_ORDER = list(DATA_SCHEMA.keys())


def _fetch_raw_benchmarks(include_history: bool) -> list[dict]:
    """
    Fetches the raw benchmark data from the Tesouro Nacional API.
    Handles network requests, retries, and basic response validation.
    """
    session = requests.Session()
    include_history_param_value = "S" if include_history else "N"
    api_endpoint = f"{API_BASE_URL}?{API_HISTORY_PARAM}={include_history_param_value}"

    try:
        response = session.get(api_endpoint, timeout=10)
        response.raise_for_status()
    except requests.exceptions.SSLError as e:
        logger.warning(
            f"SSL error encountered: {e}. Retrying without certificate verification."
        )
        response = session.get(api_endpoint, verify=False, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching benchmarks from API: {e}")
        return []  # Retorna lista vazia em caso de erro

    response_dict = response.json()

    if not response_dict or "registros" not in response_dict:
        logger.warning("API response did not contain 'registros' key or was empty.")
        return []

    return response_dict["registros"]


def _process_api_data(raw_data: list[dict]) -> pl.DataFrame:
    if not raw_data:
        return pl.DataFrame(schema=DATA_SCHEMA)

    return (
        pl.DataFrame(raw_data)
        .rename(COLUMN_MAPPING)
        .drop_nulls()
        .with_columns(pl.col("Benchmark", "BondType").str.strip_chars())
        .cast(DATA_SCHEMA)
        .sort("StartDate", "BondType", "MaturityDate")
    )


def benchmarks(bond_type: str = None, include_history: bool = False) -> pl.DataFrame:
    """Fetches benchmark data for Brazilian Treasury Bonds from the TN API.

    This function retrieves current or historical benchmark data for various Brazilian
    Treasury bond types (e.g., LTN, LFT, NTN-B). The data is sourced directly from the
    official Tesouro Nacional API.

    Args:
        include_history (bool, optional): If `True`, includes historical benchmark data.
            If `False` (default), only current benchmarks are returned.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the benchmark data.
            The DataFrame includes the following columns:
            *   `BondType` (str): The type of the bond (e.g., 'LTN', 'LFT', 'NTN-B').
            *   `MaturityDate` (datetime.date): The maturity date of the benchmark.
            *   `Benchmark` (str): The name or identifier of the benchmark
                (e.g., 'LFT 3 anos').
            *   `StartDate` (datetime.date): The start date for the benchmark's period.
            *   `EndDate` (datetime.date): The end date for the benchmark's period.

    Notes:
        *   Data is sourced from the official Tesouro Nacional (Brazilian Treasury) API.
        *   An retry mechanism is implemented for SSL certificate verification errors.
        *   The API documentation can be found at:
            https://portal-conhecimento.tesouro.gov.br/catalogo-componentes/api-leil%C3%B5es
        *   Rows with any `NaN` values are dropped before returning the DataFrame.

    Examples:
        >>> from pyield import tn
        >>> df_current = tn.benchmarks()
        >>> # Get historical benchmarks
        >>> tn.benchmarks(bond_type="LFT", include_history=True).head()
        shape: (5, 5)
        ┌──────────┬──────────────┬────────────┬────────────┬────────────┐
        │ BondType ┆ MaturityDate ┆ Benchmark  ┆ StartDate  ┆ EndDate    │
        │ ---      ┆ ---          ┆ ---        ┆ ---        ┆ ---        │
        │ str      ┆ date         ┆ str        ┆ date       ┆ date       │
        ╞══════════╪══════════════╪════════════╪════════════╪════════════╡
        │ LFT      ┆ 2020-03-01   ┆ LFT 6 anos ┆ 2014-01-01 ┆ 2014-06-30 │
        │ LFT      ┆ 2020-09-01   ┆ LFT 6 anos ┆ 2014-07-01 ┆ 2014-12-31 │
        │ LFT      ┆ 2021-03-01   ┆ LFT 6 anos ┆ 2015-01-01 ┆ 2015-04-30 │
        │ LFT      ┆ 2021-09-01   ┆ LFT 6 anos ┆ 2015-05-01 ┆ 2015-12-31 │
        │ LFT      ┆ 2022-03-01   ┆ LFT 6 anos ┆ 2016-01-01 ┆ 2016-06-30 │
        └──────────┴──────────────┴────────────┴────────────┴────────────┘
    """
    api_data = _fetch_raw_benchmarks(include_history=include_history)
    df = _process_api_data(api_data)

    # Definir a ordenação final com base no caso de uso
    if include_history:
        # Para dados históricos, a ordem cronológica é mais útil
        sort_columns = ["StartDate", "BondType", "MaturityDate"]
    else:
        # Para dados atuais, agrupar por tipo de título é mais útil
        sort_columns = ["BondType", "MaturityDate"]
        # Filtrar apenas os dados atuais
        today = clock.today()
        df = df.filter(pl.lit(today).is_between(pl.col("StartDate"), pl.col("EndDate")))

    if bond_type:
        df = df.filter(pl.col("BondType") == bond_type)

    return df.select(FINAL_COLUMN_ORDER).sort(sort_columns)
