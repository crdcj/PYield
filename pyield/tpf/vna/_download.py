"""Download e leitura das planilhas de VNA do Tesouro Nacional."""

from urllib.parse import urlparse

import polars as pl
import requests
from lxml import html

from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

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
def baixar_planilha(url_publicacao: str) -> bytes:
    """Baixa a planilha mais recente de uma publicação de VNA."""
    pagina = _buscar_conteudo(url_publicacao)
    url_planilha = _extrair_url_planilha(pagina)
    return _buscar_conteudo(url_planilha)


def ler_planilha(conteudo: bytes, aba: str) -> pl.DataFrame:
    """Lê uma aba da planilha sem interpretar seu domínio."""
    return pl.read_excel(conteudo, sheet_name=aba, has_header=False)
