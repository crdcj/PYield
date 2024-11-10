import logging
import time
from typing import Literal

import pandas as pd
import requests
from requests.exceptions import RequestException

from pyield import date_converter as dc
from pyield.date_converter import DateScalar

TIMEOUT = (5, 20)
MAX_ATTEMPTS = 5


def indicator(
    indicator_code: Literal["IPCA_MR", "SELIC_TARGET", "SELIC_OVER", "DI", "VNA_LFT"],
    reference_date: DateScalar,
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
        reference_date (DateScalar): The date for which the indicator value is
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

        >>> yd.indicator("SELIC_TARGET", "31-05-2024")
        0.105

        >>> yd.indicator("DI", "31-05-2024")
        0.104

        >>> yd.indicator("VNA_LFT", "31-05-2024")
        14903.01148

        >>> yd.indicator("SELIC_OVER", "31-05-2024")
        0.104

    """
    converted_date = dc.convert_input_dates(reference_date)
    selected_indicator_code = str(indicator_code).upper()
    match selected_indicator_code:
        case "IPCA_MR":
            return ipca_monthly_rate(converted_date)
        case "SELIC_TARGET":
            return _selic_target(converted_date)
        case "SELIC_OVER":
            return _selic_over(converted_date)
        case "DI":
            return _di(converted_date)
        case "VNA_LFT":
            return _vna_lft(converted_date)
        case _:
            raise ValueError("Invalid indicator code provided")


def ipca_monthly_rate(reference_date: pd.Timestamp) -> float:
    """
    The function makes an API call to the IBGE's data portal to retrieve the data.
    An example of the API call: https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202403/variaveis/63?localidades=N1[all]
    where '202403' is the reference date in 'YYYYMM' format.
    The API URL is constructed dynamically based on the reference date provided.
    """
    # Format the date as 'YYYYMM' for the API endpoint
    ipca_date = reference_date.strftime("%Y%m")

    # Construct the API URL using the formatted date
    api_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/{ipca_date}/variaveis/63?localidades=N1[all]"

    # Send a GET request to the API
    response = requests.get(api_url, timeout=TIMEOUT)

    # Raises HTTPError, if one occurred
    response.raise_for_status()

    # Parse the JSON response
    data = response.json()

    if not data:
        raise ValueError("No data available for the specified date")
    # Extract and return the IPCA monthly growth rate if data is available
    ipca_str = data[0]["resultados"][0]["series"][0]["serie"][ipca_date]
    return round(float(ipca_str) / 100, 4)


def _selic_target(reference_date: pd.Timestamp) -> float:
    """
    Fetches the SELIC Target rate for a specific reference date.

    The SELIC Target rate is the official rate set by the Central Bank of Brazil.

    Args:
        reference_date (pd.Timestamp): The date for which to fetch the SELIC Target.

    Returns:
        float: The SELIC Target rate as a float rounded to 4 decimal places or NaN if
        the rate is not available.
    """
    # https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    selic_date = reference_date.strftime("%d/%m/%Y")
    api_url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial={selic_date}&dataFinal={selic_date}"

    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            response = requests.get(api_url, timeout=TIMEOUT)
            response.raise_for_status()  # Raise an error for bad HTTP response

            # Try parsing JSON, raises an error if response is not valid JSON
            data = response.json()

            # Access the value directly, any issue will raise an exception
            value = data[0]["valor"]
            return round(float(value) / 100, 4)

        except (RequestException, KeyError, IndexError, ValueError) as e:
            msg = f"Error fetching SELIC Target rate (attempt {attempt + 1}): {e}"
            logging.error(msg)

        # Increment attempt count and add backoff delay
        attempt += 1
        time.sleep(1.5**attempt)  # Exponential backoff: 1.5, 2.3, 3.9, 5.1, 7.6 seconds

    # After all retries, raise an error
    raise ValueError("Failed to fetch SELIC Target rate after maximum attempts")


def _selic_over(reference_date: pd.Timestamp) -> float:
    """
    Fetches the SELIC Over rate for a specific reference date.

    The SELIC Over rate is the daily average interest rate effectively practiced
    between banks in the interbank market, using public securities as collateral.

    Args:
        reference_date (pd.Timestamp): The date for which to fetch the SELIC Over rate.

    Returns:
        float: The SELIC Over rate as a float rounded to 4 decimal places or NaN if
        the rate is not available.
    """

    formatted_date = reference_date.strftime("%d/%m/%Y")
    # https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    api_url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial={formatted_date}&dataFinal={formatted_date}"

    attempt = 0

    while attempt < MAX_ATTEMPTS:
        try:
            response = requests.get(api_url, timeout=TIMEOUT)
            response.raise_for_status()  # Handle 4xx/5xx HTTP errors

            # Try parsing JSON, raises an error if response is not valid JSON
            data = response.json()

            # Access the value directly, any issue will raise an exception
            value = data[0]["valor"]
            return round(float(value) / 100, 4)

        except (RequestException, KeyError, IndexError, ValueError) as e:
            msg = f"Error fetching SELIC Over rate (attempt {attempt + 1}): {e}"
            logging.error(msg)

        # Increment attempt count and add backoff delay
        attempt += 1
        time.sleep(1.5**attempt)  # Exponential backoff: 1.5, 2.3, 3.9, 5.1, 7.6 seconds

    # After all retries, raise an error
    raise ValueError("Failed to fetch SELIC Over rate after maximum attempts")


def _di(reference_date: pd.Timestamp) -> float:
    # https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    di_date = reference_date.strftime("%d/%m/%Y")
    api_url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={di_date}&dataFinal={di_date}"
    response = requests.get(api_url, timeout=TIMEOUT)
    response.raise_for_status()

    if di_date not in response.text:
        return float("nan")
    data = response.json()
    value = float(data[0]["valor"]) / 100  # DI daily rate
    # Annualize the daily rate
    return round((1 + value) ** 252 - 1, 4)


def _extract_vna_value(text: str) -> float:
    # Finding the part that contains the table
    start_of_table = text.find("EMISSAO")
    end_of_table = text.find("99999999*")

    # Extracting the table
    table_text = text[start_of_table:end_of_table].strip()
    table_lines = table_text.splitlines()

    # Remove empty lines
    table_lines = [line.strip() for line in table_lines if line.strip()]

    # Remove first line
    body_lines = table_lines[1:]

    vnas = []
    for line in body_lines:
        vna_str = line.split()[-1].replace(",", ".")
        vnas.append(float(vna_str))

    # Raise error if all values are not the same
    vna_value = vnas[0]
    if any(vna_value != vna for vna in vnas):
        bcb_url = "https://www.bcb.gov.br/estabilidadefinanceira/selicbaixar"
        raise ValueError(f"VNA values are not the same. Please check data at {bcb_url}")

    return vna_value


def _vna_lft(reference_date: pd.Timestamp) -> float:
    # url example: https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/20240418APC238
    url_base = "https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/"
    url_file = f"{reference_date.strftime('%Y%m%d')}APC238"
    url_vna = url_base + url_file
    session = requests.Session()

    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            response = session.get(url_vna, timeout=TIMEOUT)
            response.raise_for_status()
            return _extract_vna_value(response.text)

        except (RequestException, KeyError, IndexError, ValueError) as e:
            msg = f"Error fetching SELIC Over rate (attempt {attempt + 1}): {e}"
            logging.error(msg)

        # Increment attempt count and add backoff delay
        attempt += 1
        time.sleep(1.5**attempt)  # Exponential backoff: 1.5, 2.3, 3.9, 5.1, 7.6 seconds

    # After all retries, raise an error
    raise ValueError("Failed to fetch VNA LFT value after maximum attempts")
