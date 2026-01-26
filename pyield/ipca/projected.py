import datetime as dt
import re
from dataclasses import dataclass

import requests


@dataclass
class IndicatorProjection:
    last_updated: dt.datetime  # Date and time of the last update
    reference_period: str  # Reference month as a string in "MMM/YY" format
    projected_value: float  # Projected value


def _get_page_text() -> str:
    """
    Faz a requisição e retorna o HTML decodificado como string.
    Retornar str evita conflitos de tipo no regex.
    """
    url = "https://www.anbima.com.br/informacoes/indicadores/"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        # Decodifica explicitamente para string (latin1 conforme o header do XML)
        return r.content.decode("latin1")
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Erro ao acessar a página da ANBIMA: {e}")


def projected_rate() -> IndicatorProjection:
    """
    Retrieves the current IPCA projection from the ANBIMA website.

    This function makes an HTTP request to the ANBIMA website, extracts HTML tables
    containing economic indicators, and specifically processes the IPCA projection data.

    Process:
        1. Accesses the ANBIMA indicators webpage
        2. Extracts the third table that contains the IPCA projection
        3. Locates the row labeled as "IPCA1"
        4. Extracts the projection value and converts it to decimal format
        5. Extracts and formats the reference month of the projection
        6. Extracts the date and time of the last update

    Returns:
        IndicatorProjection: An object containing:
            - last_updated (dt.datetime): Date and time of the last data update
            - reference_period (str): Reference period of the projection as a string in
              "MMM/YY" brazilian format (e.g., "set/25")
            - projected_value (float): Projected IPCA value as a decimal number

    Raises:
        requests.RequestException: If there are connection issues with the ANBIMA site
        ValueError: If the expected data is not found in the page structure

    Example:
        >>> from pyield import ipca
        >>> # Retrieve the current IPCA projection from ANBIMA
        >>> ipca.projected_rate()
        IndicatorProjection(last_updated=..., reference_period=..., projected_value=...)

    Notes:
        - The function requires internet connection to access the ANBIMA website
        - The structure of the ANBIMA page may change, which could affect the function
    """
    # 1. Obtém o texto já decodificado (str)
    html_content = _get_page_text()

    # 2. Extrair Data de Atualização
    # Procura por: "Data e Hora da Última Atualização: 23/01/2026 - 16:48 h"
    update_pattern = r"Data e Hora da Última Atualização:\s*([0-9]{2}/[0-9]{2}/[0-9]{4}\s*-\s*[0-9]{2}:[0-9]{2})"  # noqa:E501

    match_update = re.search(update_pattern, html_content)
    if not match_update:
        raise ValueError("Não foi possível encontrar a data de atualização na página.")

    last_update_str = match_update.group(1)
    # Remove espaços extras que possam existir na captura
    last_update_str = last_update_str.replace(" - ", "-").strip()
    # Formato esperado: "23/01/2026-16:48" (ajustado para parsing seguro)
    try:
        last_updated = dt.datetime.strptime(last_update_str, "%d/%m/%Y-%H:%M")
    except ValueError:
        # Fallback caso o espaço seja mantido ou o formato varie levemente
        last_updated = dt.datetime.strptime(match_update.group(1), "%d/%m/%Y - %H:%M")

    # 3. Extrair Bloco do IPCA
    # Regex explicado:
    # IPCA.*?        -> Encontra IPCA e avança (ignora o IPCA índice, busca o próximo)
    # Projeção\s*\(  -> Encontra 'Projeção ('
    # (.*?)          -> GRUPO 1: Captura o período (ex: jan/26)
    # \)             -> Fecha parênteses
    # .*?>           -> Avança até fechar a próxima tag HTML (<td>)
    # ([0-9]+,[0-9]+)-> GRUPO 2: Captura o valor (ex: 0,36)
    # <              -> Garante que o número acabou

    ipca_pattern = r"IPCA.*?Projeção\s*\((.*?)\).*?>([0-9]+,[0-9]+)<"

    # Passamos flags= explicitamente para satisfazer linters estritos
    match_ipca = re.search(ipca_pattern, html_content, flags=re.DOTALL | re.IGNORECASE)

    if not match_ipca:
        raise ValueError("Não foi possível encontrar os dados de projeção do IPCA.")

    period_str = match_ipca.group(1)  # Ex: jan/26
    value_str = match_ipca.group(2)  # Ex: 0,36

    # Conversão de valores
    projected_value = float(value_str.replace(",", ".")) / 100
    projected_value = round(projected_value, 4)

    return IndicatorProjection(
        last_updated=last_updated,
        reference_period=period_str,
        projected_value=projected_value,
    )
