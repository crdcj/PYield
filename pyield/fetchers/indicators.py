from typing import Literal

import pandas as pd
import requests

from .. import date_converter as dc


def indicator(
    indicator_type: Literal["IPCA_MR", "SELIC_TARGET", "DI", "VNA_LFT"],
    reference_date: str | pd.Timestamp,
) -> float | None:
    reference_date = dc.convert_date(reference_date)
    ind_type = str(indicator_type).upper()
    if ind_type == "IPCA_MR":
        return ipca_monthly_rate(reference_date)
    elif ind_type == "SELIC_TARGET":
        return selic_target(reference_date)
    elif ind_type == "DI":
        return di(reference_date)
    elif ind_type == "VNA_LFT":
        return vna_lft(reference_date)
    else:
        raise ValueError(f"Invalid indicator type: {ind_type}")


def ipca_monthly_rate(reference_date: pd.Timestamp) -> float | None:
    """
    Fetches the IPCA (Índice Nacional de Preços ao Consumidor Amplo) monthly rate
    from the IBGE (Instituto Brasileiro de Geografia e Estatística) for a given
    reference date.

    Args:
        reference_date (str | pd.Timestamp): The reference date for which data
            is fetched. If a string is passed, it should be in 'DD-MM-YYYY' format.

    Returns:
        float | None: The IPCA monthly rate for the specified date as a float.
        Returns None if data is not found or in case of an error.

    Notes:
        The function makes an API call to the IBGE's data portal to retrieve the
        information. An example of the API call:
        https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202403/variaveis/63?localidades=N1[all]
        where '202403' is the reference date in 'YYYYMM' format.
        The API URL is constructed dynamically based on the reference date provided.

    Examples:
        >>> ipca_monthly_rate("01-04-2024")
        0.0038  # Indicates an IPCA monthly rate of 0.38% p.m.

    """
    # Format the date as 'YYYYMM' for the API endpoint
    ipca_date = reference_date.strftime("%Y%m")

    # Construct the API URL using the formatted date
    api_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/{ipca_date}/variaveis/63?localidades=N1[all]"

    # Send a GET request to the API
    response = requests.get(api_url, timeout=10)

    # Raises HTTPError, if one occurred
    response.raise_for_status()

    # Parse the JSON response
    data = response.json()

    # Extract and return the IPCA monthly growth rate if data is available
    if data:
        ipca_str = data[0]["resultados"][0]["series"][0]["serie"][ipca_date]
        return round(float(ipca_str) / 100, 4)
    else:
        return None


def selic_target(reference_date: pd.Timestamp) -> float | None:
    """
    Examples:
        >>> selic_taget("31-05-2024")
        0.1075  # Indicates a SELIC target rate of 10.75% p.a.
    """
    # https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    selic_date = reference_date.strftime("%d/%m/%Y")
    api_url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial={selic_date}&dataFinal={selic_date}"
    response = requests.get(api_url, timeout=10)
    response.raise_for_status()

    if selic_date in response.text:
        data = response.json()
        return round(float(data[0]["valor"]) / 100, 4)
    else:
        return None


def di(reference_date: pd.Timestamp) -> float | None:
    """
    Examples:
        >>> di("31-05-2024")
        0.00040168  # Indicates a DI daily rate of 0.02% p.d.
    """
    # https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024
    di_date = reference_date.strftime("%d/%m/%Y")
    api_url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={di_date}&dataFinal={di_date}"
    response = requests.get(api_url, timeout=10)
    response.raise_for_status()

    if di_date in response.text:
        data = response.json()
        return round(float(data[0]["valor"]) / 100, 8)
    else:
        return None


def vna_lft(reference_date: pd.Timestamp) -> float | None:
    # url example: https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/20240418APC238
    url_base = "https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/"
    url_file = f"{reference_date.strftime('%Y%m%d')}APC238"
    url_vna = url_base + url_file

    response = requests.get(url_vna, timeout=10)
    response.raise_for_status()
    file_text = response.text

    # Finding the part that contains the table
    start_of_table = file_text.find("EMISSAO")
    end_of_table = file_text.find("99999999*")

    # Extracting the table
    table_text = file_text[start_of_table:end_of_table].strip()
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
