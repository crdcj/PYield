"""Download e extração da planilha do RMD."""

import io
import zipfile as zf

import requests
from lxml import html

from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

URL_BASE = (
    "https://www.tesourotransparente.gov.br/publicacoes/relatorio-mensal-da-divida-rmd"
)
_TIMEOUT_SEGUNDOS = 60
_TTL_UM_DIA = 86_400  # segundos


@retry_padrao
def _buscar_conteudo(url: str) -> bytes:
    """Busca o conteúdo de uma URL, seguindo redirects, com retry."""
    resposta = requests.get(url, timeout=_TIMEOUT_SEGUNDOS)
    resposta.raise_for_status()
    return resposta.content


def _buscar_url_anexo() -> str:
    """Encontra a URL do arquivo ZIP do anexo mais recente do RMD."""
    conteudo_pagina = _buscar_conteudo(URL_BASE)
    arvore = html.fromstring(conteudo_pagina)
    resultado = arvore.xpath("//a[contains(@href, 'publicacao-anexo')]/@href")
    if not isinstance(resultado, list) or not resultado:
        raise ValueError("Link do anexo ZIP não encontrado na página do RMD.")
    return str(resultado[0])


def _extrair_excel(conteudo_zip: bytes) -> bytes:
    """Extrai o arquivo Excel do ZIP."""
    with zf.ZipFile(io.BytesIO(conteudo_zip), "r") as arquivo_zip:
        nomes_excel = [
            nome
            for nome in arquivo_zip.namelist()
            if nome.lower().endswith((".xlsx", ".xls"))
        ]
        if not nomes_excel:
            raise ValueError("Nenhum arquivo Excel encontrado no ZIP do RMD.")
        return arquivo_zip.read(nomes_excel[0])


@ttl_cache(ttl=_TTL_UM_DIA)
def baixar_planilha_rmd() -> bytes:
    """Baixa e extrai a planilha Excel do anexo mais recente do RMD."""
    url_anexo = _buscar_url_anexo()
    conteudo_zip = _buscar_conteudo(url_anexo)
    return _extrair_excel(conteudo_zip)
