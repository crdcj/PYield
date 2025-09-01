import logging
from datetime import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
import requests

logger = logging.getLogger(__name__)

COLUMN_MAPPING = {
    "INÍCIO": "StartDate",
    "TERMINO": "EndDate",
    "TÍTULO": "BondType",
    "VENCIMENTO": "MaturityDate",
    "BENCHMARK": "Benchmark",
}

API_BASE_URL = (
    "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/benchmarks"
)
API_HISTORY_PARAM = "incluir_historico"

TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")


def _fetch_raw_benchmarks(include_history: bool) -> list[dict]:
    """
    Fetches the raw benchmark data from the Tesouro Nacional API.
    Handles network requests, retries, and basic response validation.
    """
    session = requests.Session()
    include_history_param_value = "S" if include_history else "N"
    api_endpoint = f"{API_BASE_URL}?{API_HISTORY_PARAM}={include_history_param_value}"

    try:
        response = session.get(api_endpoint)
        response.raise_for_status()
    except requests.exceptions.SSLError as e:
        logger.warning(
            f"SSL error encountered: {e}. Retrying without certificate verification."
        )
        response = session.get(api_endpoint, verify=False)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching benchmarks from API: {e}")
        return []  # Retorna lista vazia em caso de erro

    response_dict = response.json()

    if not response_dict or "registros" not in response_dict:
        logger.warning("API response did not contain 'registros' key or was empty.")
        return []

    return response_dict["registros"]


def _process_benchmark_data(raw_data: list[dict]) -> pd.DataFrame:
    """
    Converts raw benchmark data into a cleaned and typed pandas DataFrame.
    """
    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data).dropna().convert_dtypes(dtype_backend="pyarrow")

    if df.empty:
        logger.warning("DataFrame is empty after dropping NaN values.")
        return pd.DataFrame()

    # Limpeza e tipagem
    date_cols = ["VENCIMENTO", "TERMINO", "INÍCIO"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col]).astype("date32[pyarrow]")

    str_cols = ["BENCHMARK", "TÍTULO"]
    for col in str_cols:
        df[col] = df[col].str.strip()

    return df


def benchmarks(bond_type: str = None, include_history: bool = False) -> pd.DataFrame:
    """Fetches benchmark data for Brazilian Treasury Bonds from the TN API.

    This function retrieves current or historical benchmark data for various Brazilian
    Treasury bond types (e.g., LTN, LFT, NTN-B). The data is sourced directly from the
    official Tesouro Nacional API.

    Args:
        include_history (bool, optional): If `True`, includes historical benchmark data.
            If `False` (default), only current benchmarks are returned.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the benchmark data.
            The DataFrame includes the following columns:

            *   `Benchmark` (str): The name or identifier of the benchmark
                (e.g., 'LFT 3 anos').
            *   `MaturityDate` (datetime64[ns]): The maturity date of the benchmark.
            *   `BondType` (str): The type of the bond (e.g., 'LTN', 'LFT', 'NTN-B').
            *   `StartDate` (datetime64[ns]): The start date for the benchmark's period.
            *   `EndDate` (datetime64[ns]): The end date for the benchmark's period.

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
        >>> df_history = tn.benchmarks(bond_type="LFT", include_history=True)
        >>> df_history.head()
              StartDate     EndDate BondType MaturityDate      Benchmark
        0    2014-01-01  2014-06-30      LFT   2020-03-01     LFT 6 anos
        1    2014-07-01  2014-12-31      LFT   2020-09-01     LFT 6 anos
        2    2015-01-01  2015-04-30      LFT   2021-03-01     LFT 6 anos
        3    2015-05-01  2015-12-31      LFT   2021-09-01     LFT 6 anos
        4    2016-01-01  2016-06-30      LFT   2022-03-01     LFT 6 anos
    """
    # Passo 1: Buscar os dados brutos
    raw_data = _fetch_raw_benchmarks(include_history=include_history)
    if not raw_data:
        return pd.DataFrame()  # Retorna DF vazio se a busca falhou

    # Passo 2: Processar os dados em um DataFrame limpo
    df = _process_benchmark_data(raw_data)
    if df.empty:
        return pd.DataFrame()  # Retorna DF vazio se o processamento resultou em nada

    # Passo 3: Aplicar filtros específicos da chamada
    if not include_history:
        today = dt.now(TIMEZONE_BZ).date()  # noqa
        df = df.query("INÍCIO <= @today <= TERMINO").reset_index(drop=True)
        if df.empty:
            logger.warning(
                "No current benchmark data found after filtering by active period."
            )

    if bond_type:
        df = df.query("TÍTULO == @bond_type").reset_index(drop=True)

    if df.empty:
        return pd.DataFrame()  # Retorna DF vazio se os filtros removeram tudo

    # Passo 4: Formatar a saída final
    column_order = [c for c in COLUMN_MAPPING if c in df.columns]
    return (
        df[column_order]
        .rename(columns=COLUMN_MAPPING)
        .sort_values(["BondType", "MaturityDate"])
        .reset_index(drop=True)
    )
