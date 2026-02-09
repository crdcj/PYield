import datetime as dt
import re
from dataclasses import dataclass

import requests

from pyield.retry import default_retry


@dataclass
class ProjecaoIndicador:
    ultima_atualizacao: dt.datetime  # Data e hora da última atualização
    periodo_referencia: str  # Mês de referência no formato "MMM/YY"
    valor_projetado: float  # Valor projetado


@default_retry
def _buscar_texto_pagina() -> str:
    """
    Faz a requisição e retorna o HTML decodificado como string.
    Retornar str evita conflitos de tipo no regex e facilita o processamento.
    """
    url = "https://www.anbima.com.br/informacoes/indicadores/"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        # Decodifica explicitamente para string (latin1 conforme o header do XML)
        return r.content.decode("latin1")
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Erro ao acessar a página da ANBIMA: {e}")


def projected_rate() -> ProjecaoIndicador:
    """
    Obtém a projeção atual do IPCA no site da ANBIMA.

    A função acessa a página de indicadores da ANBIMA e extrai os dados da
    projeção do IPCA diretamente do HTML usando expressões regulares.

    Returns:
        ProjecaoIndicador: Objeto contendo:
            - ultima_atualizacao (dt.datetime): Data e hora da última atualização.
            - periodo_referencia (str): Período de referência no formato "MMM/YY"
              (ex.: "jan/26").
            - valor_projetado (float): Valor projetado do IPCA (decimal).

    Raises:
        ConnectionError: Se houver erro de conexão com o site da ANBIMA.
        ValueError: Se os padrões esperados não forem encontrados no HTML.

    Notes:
        - Requer conexão com a internet para acessar o site da ANBIMA.
        - A extração depende de padrões de texto no HTML. Mudanças na estrutura
          da página podem afetar o resultado.

    Examples:
        >>> from pyield import ipca
        >>> # Obter a projeção atual do IPCA na ANBIMA
        >>> ipca.projected_rate()
        ProjecaoIndicador(ultima_atualizacao=..., periodo_referencia=..., valor_projetado=...)
    """  # noqa:E501
    # 1. Obtém o texto já decodificado (str)
    html = _buscar_texto_pagina()

    # 2. Extrair Data de Atualização
    # Procura por: "Data e Hora da Última Atualização: 23/01/2026 - 16:48 h"
    padrao_atualizacao = r"Data e Hora da Última Atualização:\s*([0-9]{2}/[0-9]{2}/[0-9]{4}\s*-\s*[0-9]{2}:[0-9]{2})"  # noqa:E501

    correspondencia_atualizacao = re.search(padrao_atualizacao, html)
    if not correspondencia_atualizacao:
        raise ValueError("Não foi possível encontrar a data de atualização na página.")

    texto_atualizacao = correspondencia_atualizacao.group(1)
    # Remove espaços extras que possam existir na captura
    texto_atualizacao = texto_atualizacao.replace(" - ", "-").strip()

    # Formato esperado: "23/01/2026-16:48"
    try:
        ultima_atualizacao = dt.datetime.strptime(texto_atualizacao, "%d/%m/%Y-%H:%M")
    except ValueError:
        # Fallback caso o espaço seja mantido ou o formato varie levemente
        ultima_atualizacao = dt.datetime.strptime(
            correspondencia_atualizacao.group(1), "%d/%m/%Y - %H:%M"
        )

    # 3. Extrair Bloco do IPCA
    # Regex explicado:
    # IPCA.*?         -> Encontra IPCA e avança (ignora o IPCA índice, busca o próximo)
    # Projeção\s*\(   -> Encontra "Projeção ("
    # (.*?)           -> GRUPO 1: Captura o período (ex.: jan/26)
    # \)              -> Fecha parênteses
    # .*?>            -> Avança até fechar a próxima tag HTML (<td>)
    # ([0-9]+,[0-9]+) -> GRUPO 2: Captura o valor (ex.: 0,36)
    # <               -> Garante que o número acabou

    padrao_ipca = r"IPCA.*?Projeção\s*\((.*?)\).*?>([0-9]+,[0-9]+)<"

    # Passamos flags= explicitamente para satisfazer linters estritos
    correspondencia_ipca = re.search(padrao_ipca, html, flags=re.DOTALL | re.IGNORECASE)

    if not correspondencia_ipca:
        raise ValueError("Não foi possível encontrar os dados de projeção do IPCA.")

    periodo_referencia = correspondencia_ipca.group(1)  # Ex.: jan/26
    texto_valor = correspondencia_ipca.group(2)  # Ex.: 0,36

    # Conversão de valores
    valor_projetado = float(texto_valor.replace(",", ".")) / 100
    valor_projetado = round(valor_projetado, 4)

    return ProjecaoIndicador(
        ultima_atualizacao=ultima_atualizacao,
        periodo_referencia=periodo_referencia,
        valor_projetado=valor_projetado,
    )
