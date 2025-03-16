import requests

from pyield.date_converter import DateScalar, convert_input_dates
from pyield.retry import default_retry


@default_retry
def _get_text(date: DateScalar) -> str:
    # url example: https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/20240418APC238
    url_base = "https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/"
    date = convert_input_dates(date)
    url_file = f"{date.strftime('%Y%m%d')}APC238"
    url_vna = url_base + url_file

    response = requests.get(url_vna)
    response.raise_for_status()
    return response.text


def _extract_vna_table_text(text: str) -> str:
    """Extrai o texto contendo a tabela VNA do texto bruto."""
    start_of_table = text.find("EMISSAO")
    end_of_table = text.find("99999999*")
    table_text = text[start_of_table:end_of_table].strip()
    return table_text


def _parse_vna_table_lines(table_text: str) -> list[str]:
    """Processa o texto da tabela VNA para retornar uma lista de linhas."""
    table_lines = table_text.splitlines()
    table_lines = [
        line.strip() for line in table_lines if line.strip()
    ]  # Remove empty lines
    body_lines = table_lines[1:]  # Remove first line (header)
    return body_lines


def _extract_vna_values_from_lines(body_lines: list[str]) -> list[float]:
    """Extrai valores VNA numéricos de uma lista de linhas de texto."""
    vnas = []
    for line in body_lines:
        vna_str = line.split()[-1].replace(",", ".")
        vnas.append(float(vna_str))
    return vnas


def _validate_vna_values(vnas: list[float]) -> float:
    """Valida se todos os valores VNA são iguais e retorna o valor único."""
    vna_value = vnas[0]
    if any(vna_value != vna for vna in vnas):
        bcb_url = "https://www.bcb.gov.br/estabilidadefinanceira/selicbaixar"
        msg = f"VNA values are not the same. Please check data at {bcb_url}"
        raise ValueError(msg)
    return vna_value


def vna_lft(date: DateScalar) -> float:
    """Retrieves the VNA (Valor Nominal Atualizado) from the BCB for a given date.

    This function fetches daily data from the BCB website, extracts the
    VNA value from a specific table within the downloaded content, and
    returns this value.

    Args:
        date (DateScalar): The date for which to retrieve the VNA value.
            This argument accepts various date formats, including string and
            datetime objects, which are then standardized using the
            `convert_input_dates` function.

    Returns:
        float: The VNA (Valor Nominal Atualizado) value for the specified date.

    Examples:
        >>> from pyield import bc
        >>> bc.vna_lft("31-05-2024")
        14903.01148

    Raises:
        ValueError: If the extracted VNA values from the BCB website are
            inconsistent (i.e., not all extracted values are identical),
            suggesting potential data discrepancies on the source website.
            The error message includes a link to the BCB website for manual
            verification.
        requests.exceptions.HTTPError: If the HTTP request to the BCB website
            fails. This could be due to network issues, website unavailability,
            or the requested data not being found for the given date.
    """
    text = _get_text(date)
    table_text = _extract_vna_table_text(text)
    table_lines = _parse_vna_table_lines(table_text)
    vnas = _extract_vna_values_from_lines(table_lines)
    vna_value = _validate_vna_values(vnas)
    return vna_value
