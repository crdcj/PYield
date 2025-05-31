"""
Manual da API: https://portal-conhecimento.tesouro.gov.br/catalogo-componentes/api-leil%C3%B5es
Financeiro é sempre o financeiro aceito.
Os dados do BCB só existem na API do TN.
"""

import logging

import pandas as pd
import requests

BENCHMARKS_COLUMN_MAPPING = {
    "BENCHMARK": "Benchmark",
    "VENCIMENTO": "MaturityDate",
    "TÍTULO": "BondType",
    "INÍCIO": "StartDate",
    "TERMINO": "EndDate",
}


API_URL = "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/benchmarks?incluir_historico="


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
        >>> # Get current benchmarks
        >>> from pyield import tn
        >>> df_current = tn.benchmarks()

        >>> # Get historical benchmarks
        >>> df_history = benchmarks(include_history=True)
        >>> df_history.head()
            Benchmark MaturityDate BondType  StartDate    EndDate
        0  LFT 6 anos   2020-03-01      LFT 2014-01-01 2014-06-30
        1  LFT 6 anos   2020-09-01      LFT 2014-07-01 2014-12-31
        2  LFT 6 anos   2021-03-01      LFT 2015-01-01 2015-04-30
        3  LFT 6 anos   2021-09-01      LFT 2015-05-01 2015-12-31
        4   LFT 1 ano   2022-03-01      LFT 2020-10-15 2021-03-31
    """
    session = requests.Session()
    include_history_param = "S" if include_history else "N"
    api_endpoint = API_URL + include_history_param
    try:
        stn_benchmarks = session.get(api_endpoint)
    except requests.exceptions.SSLError as e:
        logging.error(
            f"SSL error encountered: {e}. Retrying without certificate verification."
        )
        stn_benchmarks = session.get(api_endpoint, verify=False)
    response_dict = stn_benchmarks.json()
    df = pd.DataFrame(response_dict["registros"]).dropna()
    df = df.convert_dtypes()
    # A API pode retornar vazio, então é necessário verificar
    if not df.empty:
        df["VENCIMENTO"] = pd.to_datetime(df["VENCIMENTO"])
        df["TERMINO"] = pd.to_datetime(df["TERMINO"])
        df["INÍCIO"] = pd.to_datetime(df["INÍCIO"])
        df["BENCHMARK"] = df["BENCHMARK"].str.strip()
        df["TÍTULO"] = df["TÍTULO"].str.strip()
    else:
        logging.warning("Não foi possível obter os benchmarks do STN.")

    return df.rename(columns=BENCHMARKS_COLUMN_MAPPING)
