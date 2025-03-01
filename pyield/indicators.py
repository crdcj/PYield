import logging
import time
from typing import Callable, Literal

import pandas as pd
import requests
from requests.exceptions import RequestException

from pyield import date_converter as dc
from pyield.date_converter import DateScalar

logger = logging.getLogger(__name__)

# Timeout para as requisições HTTP (conexão e leitura)
TIMEOUT = (5, 20)
MAX_ATTEMPTS = 10


def indicator(
    indicator_code: Literal["IPCA_MR", "SELIC_TARGET", "SELIC_OVER", "DI", "VNA_LFT"],
    date: DateScalar,
) -> float:
    """
    Fetches the economic indicator value for a specified reference date.

    This function retrieves the value of a specified economic indicator, such as IPCA
    (monthly inflation), SELIC (target or overnight rate), DI (interbank deposit rate),
    or the VNA of LFT (Valor Nominal Atualizado), based on the given reference date. The
    correct API is dynamically chosen based on the indicator code provided.

    Args:
        indicator_code (IndicatorCode): The code of the economic indicator to fetch.
            The available options are:
            - "IPCA_MR": IPCA Monthly Rate (inflation).
            - "SELIC_TARGET": SELIC Target rate.
            - "SELIC_OVER": SELIC Over (overnight) rate.
            - "DI": DI (interbank deposit rate).
            - "VNA_LFT": Valor Nominal Atualizado for LFT (Treasury Bills).
        date (DateScalar): The date for which the indicator value is
            fetched. If passed as a string, it should be in 'DD-MM-YYYY' format.

    Returns:
        float: The value of the requested economic indicator for the specified date.
        Returns NaN if the value cannot be retrieved or an error occurs.

    Raises:
        ValueError: If an invalid `indicator_code` is provided.
        ValueError: After all retries, if the indicator value cannot be fetched.

    Examples:
        >>> yd.indicator("IPCA_MR", "01-04-2024")
        0.0038
    """
    converted_date = dc.convert_input_dates(date)
    selected_indicator_code = str(indicator_code).upper()
    match selected_indicator_code:
        case "IPCA_MR":
            return ipca_monthly_rate(converted_date)
        case _:
            raise ValueError("Invalid indicator code provided")


def ipca_monthly_rate(date: pd.Timestamp) -> float:
    """
    The function makes an API call to the IBGE's data portal to retrieve the data.
    An example of the API call: https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202403/variaveis/63?localidades=N1[all]
    where '202403' is the reference date in 'YYYYMM' format.
    The API URL is constructed dynamically based on the reference date provided.
    """
    # Format the date as 'YYYYMM' for the API endpoint
    ipca_date = date.strftime("%Y%m")

    # Construct the API URL using the formatted date
    api_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/{ipca_date}/variaveis/63?localidades=N1[all]"

    def process_ipca_response(data):
        if not data:
            msg = f"No data available for IPCA Monthly Rate on {date}"
            logger.warning(msg)
            return float("nan")
        ipca_str = data[0]["resultados"][0]["series"][0]["serie"][ipca_date]
        return round(float(ipca_str) / 100, 4)

    return _fetch_with_retry(api_url, "IPCA Monthly Rate", process_ipca_response, True)


def _fetch_with_retry(
    api_url: str,
    error_message: str,
    process_response: Callable[[requests.Response], float],
    use_json: bool = True,
) -> float:
    """
    Função genérica para fazer requisições HTTP com retentativas e tratamento de erro.

    Args:
        api_url (str): URL da API a ser chamada.
        error_message (str): Mensagem de erro específica para a função que está usando.
        process_response (Callable[[str], float]): Função para processar os dados da
            resposta (JSON decodificado ou texto, dependendo de 'use_json').
            Deve retornar um float.
        use_json (bool): Se True, tenta decodificar a resposta como JSON antes de passar
            para `process_response`. Se False, passa a resposta como texto.

    Returns:
        float: O resultado do processamento da resposta.

    Raises:
        ValueError: Se a requisição falhar após o número máximo de tentativas.
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            response = requests.get(api_url, timeout=TIMEOUT)
            response.raise_for_status()  # Levanta uma exceção para códigos de erro HTTP

            if use_json:
                data = response.json()
            else:
                data = response.text

            return process_response(data)

        except RequestException as e:
            msg = f"Erro ao buscar {error_message} (tentativa {attempt + 1}): "

            if hasattr(e, "response"):
                response = e.response
                if response is not None:  # Verificação extra se response não é None
                    status_code = response.status_code
                    msg += f"Status Code: {status_code}, "
                    if status_code == requests.codes.not_found:
                        msg = f"Sem dado para {error_message} na data solicitada."
                        msg += f"API URL é: {api_url}"
                        logger.warning(msg)
                        return float("nan")
            else:
                status_code = "N/A (response is None ou response attribute não existe)"

            msg += f"Exceção: {e}, Tipo da Exceção: {type(e)}"
            msg += f" , Response Object Existe? {hasattr(e, 'response')}"

            logger.error(msg)

        except Exception as unhandled_error:
            msg = f"Erro INESPERADO ao buscar {error_message}: {unhandled_error}"
            logger.exception(msg)  # Loga stack trace completo para debug

        attempt += 1
        time.sleep(1.2**attempt)  # Backoff exponencial

    msg = f"Falha ao buscar {error_message} após {MAX_ATTEMPTS} tentativas."
    msg += f" API URL: {api_url}"
    raise ValueError(msg)
