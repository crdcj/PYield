"""Testes do módulo rmd.

Baixa o ZIP do release de teste no GitHub e valida o pipeline completo
contra o parquet de referência local.
"""

import sys
from functools import lru_cache
from pathlib import Path

import polars as pl
import requests

import pyield.rmd  # noqa: F401 — garante que o módulo está carregado

rmd_mod = sys.modules["pyield.rmd"]

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_PARQUET = DIRETORIO_DADOS / "rmd_1.3.parquet"

URL_BASE_RELEASE = "https://github.com/crdcj/PYield/releases/download/test-data"
NOME_ZIP = "Anexo.RMD_Janeiro_26.zip"


@lru_cache(maxsize=1)
def _baixar_zip_remoto() -> bytes:
    """Baixa o ZIP do RMD do release de teste (com cache em memória)."""
    url = f"{URL_BASE_RELEASE}/{NOME_ZIP}"
    resposta = requests.get(url, timeout=(5, 60))
    resposta.raise_for_status()
    return resposta.content


def test_pipeline_rmd(monkeypatch):
    """Testa o pipeline completo: ZIP → Excel → DataFrame."""
    conteudo_zip = _baixar_zip_remoto()

    # Substitui as duas funções de rede: busca da URL e download do ZIP
    monkeypatch.setattr(rmd_mod, "_buscar_url_anexo", lambda: "http://fake")
    monkeypatch.setattr(rmd_mod, "_buscar_conteudo", lambda url: conteudo_zip)

    resultado = rmd_mod.rmd(aba="1.3")
    esperado = pl.read_parquet(CAMINHO_PARQUET)
    colunas_ordem = ["periodo", "grupo", "subgrupo", "titulo", "valor"]
    assert resultado.sort(colunas_ordem).equals(esperado)
