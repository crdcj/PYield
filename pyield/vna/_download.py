"""Download e leitura das planilhas de VNA do Tesouro Nacional."""

import polars as pl
import requests

from pyield._internal.cache import ttl_cache
from pyield._internal.excel import ler_sem_cabecalho
from pyield._internal.retry import retry_padrao

_TIMEOUT_SEGUNDOS = 60


@retry_padrao
def _buscar_conteudo(url: str) -> bytes:
    """Busca o conteúdo bruto de uma URL do Tesouro Nacional."""
    resposta = requests.get(url, timeout=_TIMEOUT_SEGUNDOS)
    resposta.raise_for_status()
    return resposta.content


@ttl_cache()
def baixar_planilha(url_arquivo: str) -> bytes:
    """Baixa a planilha pelo link permanente da publicação."""
    return _buscar_conteudo(url_arquivo)


def ler_planilha(conteudo: bytes, aba: str) -> pl.DataFrame:
    """Lê uma aba da planilha sem interpretar seu domínio."""
    return ler_sem_cabecalho(conteudo, aba)
