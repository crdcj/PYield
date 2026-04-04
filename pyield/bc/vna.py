"""VNA (Valor Nominal Atualizado) da LFT no site do BCB.

Exemplo de chamada à API:
    https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/20240531APC238

Trecho relevante da resposta (tabela VNA):
    EMISSAO     VENCIMENTO   DATA BASE    TITULO        INDICE
    03/02/2021  01/09/2024   01/07/2000   210100       14903,011480
    30/03/2022  01/03/2025   01/07/2000   210100       14903,011480
    23/08/2019  01/09/2025   01/07/2000   210100       14903,011480
    28/06/2023  01/03/2026   01/07/2000   210100       14903,011480
"""

import requests

from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty


@ttl_cache()
@retry_padrao
def _baixar_texto(data_referencia: DateLike) -> str:
    """Baixa o arquivo diário do SELIC no site do BCB."""
    # Exemplo: https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/20240418APC238
    url_base = "https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/"
    data = converter_datas(data_referencia)
    url_file = f"{data.strftime('%Y%m%d')}APC238"
    url = url_base + url_file

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text


def _recortar_tabela(texto: str) -> str:
    """Recorta o trecho da tabela VNA do texto bruto."""
    inicio = texto.find("EMISSAO")
    fim = texto.find("99999999*")
    return texto[inicio:fim].strip()


def _obter_linhas(texto_tabela: str) -> list[str]:
    """Retorna as linhas de dados (sem cabeçalho) da tabela VNA."""
    todas = texto_tabela.splitlines()
    linhas = [linha.strip() for linha in todas if linha.strip()]
    return linhas[1:]


def _extrair_valores(linhas: list[str]) -> list[float]:
    """Extrai os valores VNA numéricos das linhas de texto."""
    valores = []
    for linha in linhas:
        vna_str = linha.split()[-1].replace(",", ".")
        valores.append(float(vna_str))
    return valores


def _validar_valores(valores: list[float]) -> float:
    """Valida se todos os valores VNA são iguais e retorna o valor único."""
    valor = valores[0]
    if any(valor != v for v in valores):
        bcb_url = "https://www.bcb.gov.br/estabilidadefinanceira/selicbaixar"
        msg = f"Valores VNA divergentes. Verifique os dados em {bcb_url}"
        raise ValueError(msg)
    return valor


def vna_lft(data: DateLike) -> float:
    """Obtém o VNA (Valor Nominal Atualizado) da LFT no site do BCB.

    Baixa o arquivo diário do BCB (SELIC), extrai a tabela com os valores
    VNA e retorna o valor correspondente à data informada.

    Args:
        data: Data da consulta. Aceita string, date ou datetime,
            convertidos internamente por ``converter_datas``.

    Returns:
        float: Valor do VNA para a data especificada. Retorna ``NaN`` se a
            data for nula ou vazia.

    Raises:
        ValueError: Se os valores VNA extraídos do site do BCB forem
            inconsistentes (nem todos iguais), indicando possível divergência
            nos dados da fonte. A mensagem inclui o link do BCB para
            verificação manual.
        requests.exceptions.HTTPError: Se a requisição HTTP ao site do BCB
            falhar (problemas de rede, site indisponível ou dados não
            encontrados para a data informada).

    Examples:
        >>> from pyield import bc
        >>> bc.vna_lft("31-05-2024")
        14903.01148
    """
    if any_is_empty(data):
        return float("nan")
    texto = _baixar_texto(data)
    tabela = _recortar_tabela(texto)
    linhas = _obter_linhas(tabela)
    valores = _extrair_valores(linhas)
    return _validar_valores(valores)
