"""Testes do pipeline tpf.rmd().

Baixa o ZIP do release de teste no GitHub e valida o pipeline completo
contra o parquet de referência local.
"""

import importlib
from functools import lru_cache
from pathlib import Path

import polars as pl
import requests

modulo_rmd = importlib.import_module("pyield.tn.rmd")

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
    """tpf.rmd() com monkeypatch deve bater com o parquet de referência."""
    conteudo_zip = _baixar_zip_remoto()

    monkeypatch.setattr(modulo_rmd, "_buscar_url_anexo", lambda: "http://fake")
    monkeypatch.setattr(modulo_rmd, "_buscar_conteudo", lambda url: conteudo_zip)

    resultado = modulo_rmd.rmd(aba="1.3")
    esperado = pl.read_parquet(CAMINHO_PARQUET)
    colunas_ordem = ["periodo", "grupo", "subgrupo", "titulo", "valor"]
    assert resultado.sort(colunas_ordem).equals(esperado)


def test_totais_2025_rmd(monkeypatch):
    """Totais de 2025 devem bater com os valores de referência do Tesouro Nacional."""
    conteudo_zip = _baixar_zip_remoto()

    monkeypatch.setattr(modulo_rmd, "_buscar_url_anexo", lambda: "http://fake")
    monkeypatch.setattr(modulo_rmd, "_buscar_conteudo", lambda url: conteudo_zip)

    ano_ref = 2025
    emissoes_esperadas = 1_840_946_621_648.18
    resgates_esperados = 1_395_109_062_272.45

    df = modulo_rmd.rmd(aba="1.3")
    df_2025 = df.filter(pl.col("periodo").dt.year() == ano_ref)

    emissoes = df_2025.filter(pl.col("grupo") == "Emissões")["valor"].sum()
    resgates = df_2025.filter(pl.col("grupo") == "Resgates")["valor"].sum()

    assert round(emissoes, 2) == emissoes_esperadas
    assert round(resgates, 2) == resgates_esperados
