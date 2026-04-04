import datetime as dt
from dataclasses import dataclass

import polars as pl
import requests

from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

_URL_XLS = "https://www.anbima.com.br/informacoes/indicadores/arqs/indicadores.xls"


@dataclass(frozen=True)
class ProjecaoIndicador:
    ultima_atualizacao: dt.datetime  # Data e hora da última atualização
    periodo_referencia: str  # Mês de referência no formato "MMM/YY"
    valor_projetado: float  # Valor projetado


@ttl_cache()
@retry_padrao
def _baixar_planilha() -> bytes:
    """Baixa o arquivo XLS de indicadores da ANBIMA e retorna os bytes."""
    try:
        r = requests.get(_URL_XLS, timeout=10)
        r.raise_for_status()
        return r.content
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Erro ao acessar a planilha da ANBIMA: {e}")


def _extrair_datetime(texto: str) -> dt.datetime:
    """Extrai datetime do texto da primeira célula do XLS.

    Formato esperado:
    "Data e Hora da Última Atualização: 13/03/2026 - 15:20 h"
    """
    # Prefixo fixo → restante é " 13/03/2026 - 15:20 h"
    prefixo = "Data e Hora da Última Atualização:"
    parte_data = texto.split(prefixo, maxsplit=1)[1]
    parte_data = parte_data.strip().removesuffix("h").strip()
    return dt.datetime.strptime(parte_data, "%d/%m/%Y - %H:%M")


def _extrair_periodo(texto: str) -> str:
    """Extrai o período de referência de 'Projeção (mar/26)' → 'mar/26'."""
    inicio = texto.index("(") + 1
    fim = texto.index(")")
    return texto[inicio:fim]


def taxa_projetada() -> ProjecaoIndicador:
    """
    Obtém a projeção atual do IPCA no site da ANBIMA.

    A função baixa a planilha XLS de indicadores da ANBIMA e extrai os
    dados da projeção do IPCA.

    Returns:
        ProjecaoIndicador: Objeto contendo:
            - ultima_atualizacao (dt.datetime): Data e hora da última
              atualização.
            - periodo_referencia (str): Período de referência no formato
              "MMM/YY" (ex.: "mar/26").
            - valor_projetado (float): Valor projetado do IPCA (decimal).

    Raises:
        ConnectionError: Se houver erro de conexão com o site da ANBIMA.
        ValueError: Se os dados esperados não forem encontrados na
            planilha.

    Notes:
        Requer conexão com a internet para acessar o site da ANBIMA.

    Examples:
        >>> from pyield import ipca
        >>> # Obter a projeção atual do IPCA na ANBIMA
        >>> ipca.taxa_projetada()
        ProjecaoIndicador(ultima_atualizacao=..., periodo_referencia=..., valor_projetado=...)
    """
    conteudo = _baixar_planilha()
    df = pl.read_excel(conteudo, has_header=False)

    # Linha 0: "Data e Hora da Última Atualização: DD/MM/YYYY - HH:MM h"
    texto_atualizacao = df.item(0, "column_1")
    ultima_atualizacao = _extrair_datetime(texto_atualizacao)

    # Linha do IPCA: column_1 começa com "IPCA" e column_2 com "Projeção"
    linha_ipca = df.filter(
        pl.col("column_1").str.starts_with("IPCA")
        & pl.col("column_2").str.starts_with("Projeção")
    )
    if linha_ipca.is_empty():
        raise ValueError("Não foi possível encontrar a projeção do IPCA na planilha.")

    periodo_referencia = _extrair_periodo(linha_ipca.item(0, "column_2"))
    valor_projetado = round(float(linha_ipca.item(0, "column_3")) / 100, 4)

    return ProjecaoIndicador(
        ultima_atualizacao=ultima_atualizacao,
        periodo_referencia=periodo_referencia,
        valor_projetado=valor_projetado,
    )
