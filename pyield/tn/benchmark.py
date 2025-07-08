import logging
from datetime import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
import requests

logger = logging.getLogger(__name__)

TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")

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


def benchmarks(include_history: bool = False) -> pd.DataFrame:
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
        >>> # Get current benchmarks (default behavior)
        >>> from pyield import tn
        >>> df_current = tn.benchmarks()

        >>> # Get historical benchmarks
        >>> df_history = tn.benchmarks(include_history=True)
        >>> df_history.head()
           StartDate    EndDate BondType MaturityDate     Benchmark
        0 2014-01-01 2014-06-30      LFT   2020-03-01    LFT 6 anos
        1 2014-01-01 2014-06-01      LTN   2014-10-01   LTN 6 meses
        2 2014-01-01 2014-06-30      LTN   2015-04-01  LTN 12 meses
        3 2014-01-01 2014-06-30      LTN   2016-04-01  LTN 24 meses
        4 2014-01-01 2014-06-30      LTN   2018-01-01  LTN 48 meses
    """
    session = requests.Session()
    include_history_param_value = "S" if include_history else "N"
    api_endpoint = f"{API_BASE_URL}?{API_HISTORY_PARAM}={include_history_param_value}"

    try:
        stn_benchmarks = session.get(api_endpoint)
        stn_benchmarks.raise_for_status()
    except requests.exceptions.SSLError as e:
        logger.error(
            f"SSL error encountered: {e}. Retrying without certificate verification."
        )
        stn_benchmarks = session.get(api_endpoint, verify=False)
        stn_benchmarks.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching benchmarks from API: {e}")
        return pd.DataFrame()

    response_dict = stn_benchmarks.json()

    # 1a verificação: Verifica se a resposta da API contém dados válidos de registros
    if not response_dict or not response_dict.get("registros"):
        logger.warning("API response did not contain 'registros' key or it was empty.")
        return pd.DataFrame()

    # Tenta criar o DataFrame. O .dropna() pode resultar em um DF vazio.
    df = pd.DataFrame(response_dict["registros"]).dropna()
    df = df.convert_dtypes()

    # 2a verificação: Verifica se o DataFrame resultante (pós-dropna) está vazio
    # Esta verificação é importante porque .dropna() pode remover todas as linhas.
    if df.empty:
        logger.warning(
            "No valid benchmark data found after initial processing"
            "(e.g., all rows had NaNs and were dropped).",
        )
        return pd.DataFrame()

    # Se chegamos até aqui, o DataFrame tem dados e pode ser processado.
    df["VENCIMENTO"] = pd.to_datetime(df["VENCIMENTO"])
    df["TERMINO"] = pd.to_datetime(df["TERMINO"])
    df["INÍCIO"] = pd.to_datetime(df["INÍCIO"])
    df["BENCHMARK"] = df["BENCHMARK"].str.strip()
    df["TÍTULO"] = df["TÍTULO"].str.strip()

    if not include_history:
        # Em tese, a API já retorna apenas benchmarks ativos,
        # mas vamos garantir que o DataFrame só contenha benchmarks ativos
        # considerando o período atual.
        today = dt.now(TIMEZONE_BZ).date()
        today = pd.Timestamp(today)
        df = df.query("INÍCIO <= @today <= TERMINO").reset_index(drop=True)

    # Verifica novamente se o DataFrame ficou vazio *após o filtro condicional*
    # (apenas se `include_history` for False)
    if df.empty and not include_history:
        logger.warning(
            "No current benchmark data found after filtering by active period."
        )

    column_order = [c for c in COLUMN_MAPPING if c in df.columns]
    return (
        df[column_order]
        .rename(columns=COLUMN_MAPPING)
        .sort_values(["StartDate", "BondType", "MaturityDate"])
        .reset_index(drop=True)
    )
