"""Download e leitura das planilhas de VNA do Tesouro Nacional."""

import logging
from urllib.parse import urlparse

import polars as pl
import requests
from lxml import html

from pyield._internal.cache import ttl_cache
from pyield._internal.excel import ler_sem_cabecalho
from pyield._internal.retry import retry_padrao

registro = logging.getLogger(__name__)
_DOMINIO_ARQUIVOS = "thot-arquivos.tesouro.gov.br"
_TIMEOUT_SEGUNDOS = 60


@retry_padrao
def _buscar_conteudo(url: str) -> bytes:
    """Busca o conteúdo bruto de uma URL do Tesouro Nacional."""
    resposta = requests.get(url, timeout=_TIMEOUT_SEGUNDOS)
    resposta.raise_for_status()
    return resposta.content


def _extrair_url_planilha(conteudo: bytes) -> str:
    """Extrai da página de publicação a URL da planilha mais recente."""
    arvore = html.fromstring(conteudo)
    urls = arvore.xpath("//a[@href]/@href")
    if isinstance(urls, list):
        for resultado in urls:
            url = str(resultado)
            partes = urlparse(url)
            if (
                partes.hostname == _DOMINIO_ARQUIVOS
                and partes.path.startswith("/publicacao/")
            ):
                return url

    msg = "Planilha de VNA não encontrada no Tesouro Transparente."
    raise ValueError(msg)


@ttl_cache()
def baixar_planilha(url_publicacao: str, url_arquivo: str | None = None) -> bytes:
    """Baixa a planilha por permalink ou descobre o link na publicação."""
    if url_arquivo is not None:
        try:
            return _buscar_conteudo(url_arquivo)
        except requests.RequestException:
            registro.warning(
                "Falha ao baixar o permalink da planilha de VNA; "
                "tentando descobrir o arquivo pela página de publicação."
            )

    pagina = _buscar_conteudo(url_publicacao)
    url_planilha = _extrair_url_planilha(pagina)
    return _buscar_conteudo(url_planilha)


def ler_planilha(conteudo: bytes, aba: str) -> pl.DataFrame:
    """Lê uma aba da planilha sem interpretar seu domínio."""
    return ler_sem_cabecalho(conteudo, aba)
