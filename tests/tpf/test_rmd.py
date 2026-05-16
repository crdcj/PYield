"""Testes do pipeline tpf.rmd().

Baixa o ZIP do release de teste no GitHub e valida o pipeline completo
contra o parquet de referência local.
"""

import importlib
import io
import zipfile as zf
from functools import lru_cache
from pathlib import Path

import polars as pl
import requests

modulo_rmd = importlib.import_module("pyield.tpf.rmd")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_PARQUET = DIRETORIO_DADOS / "rmd_1.3.parquet"
CAMINHO_XLSX_EXEMPLO = (
    Path(__file__).parents[2] / "dev" / "rmd" / "Anexo RMD_Março_26.xlsx"
)

URL_BASE_RELEASE = "https://github.com/crdcj/PYield/releases/download/test-data"
NOME_ZIP = "Anexo.RMD_Janeiro_26.zip"
TOTAL_PUBLICO_MAR_26 = 8_633_441_170_321.0
LFT_TN_MAR_26 = 4_116_521_969_888.0
GLOBAL_USD_MAR_26 = 277_468_888_502.0
BC_TOTAL_MAR_26 = 2_893_223_808_917.0


@lru_cache(maxsize=1)
def _baixar_zip_remoto() -> bytes:
    """Baixa o ZIP do RMD do release de teste (com cache em memória)."""
    url = f"{URL_BASE_RELEASE}/{NOME_ZIP}"
    resposta = requests.get(url, timeout=(5, 60))
    resposta.raise_for_status()
    return resposta.content


def _extrair_excel_do_zip(conteudo_zip: bytes) -> bytes:
    """Extrai a planilha Excel do ZIP remoto de teste."""
    with zf.ZipFile(io.BytesIO(conteudo_zip), "r") as arquivo_zip:
        nome_excel = next(
            nome for nome in arquivo_zip.namelist() if nome.lower().endswith(".xlsx")
        )
        return arquivo_zip.read(nome_excel)


def test_pipeline_rmd(monkeypatch):
    """tpf.rmd() com monkeypatch deve bater com o parquet de referência."""
    conteudo_excel = _extrair_excel_do_zip(_baixar_zip_remoto())

    monkeypatch.setattr(modulo_rmd, "_carregar_planilha_rmd", lambda: conteudo_excel)

    resultado = modulo_rmd.rmd(aba="1.3")
    esperado = pl.read_parquet(CAMINHO_PARQUET)
    colunas_ordem = ["periodo", "grupo", "subgrupo", "titulo", "valor"]
    assert resultado.sort(colunas_ordem).equals(esperado)


def test_totais_2025_rmd(monkeypatch):
    """Totais de 2025 devem bater com os valores de referência do Tesouro Nacional."""
    conteudo_excel = _extrair_excel_do_zip(_baixar_zip_remoto())

    monkeypatch.setattr(modulo_rmd, "_carregar_planilha_rmd", lambda: conteudo_excel)

    ano_ref = 2025
    emissoes_esperadas = 1_840_946_621_648.18
    resgates_esperados = 1_395_109_062_272.45

    df = modulo_rmd.rmd(aba="1.3")
    df_2025 = df.filter(pl.col("periodo").dt.year() == ano_ref)

    emissoes = df_2025.filter(pl.col("grupo") == "Emissões")["valor"].sum()
    resgates = df_2025.filter(pl.col("grupo") == "Resgates")["valor"].sum()

    assert round(emissoes, 2) == emissoes_esperadas
    assert round(resgates, 2) == resgates_esperados


def test_aba_2_1_estrutura_e_valores(monkeypatch):
    """A aba 2.1 deve retornar estoque plano (somente folhas) com valores em R$."""
    conteudo_excel = CAMINHO_XLSX_EXEMPLO.read_bytes()
    monkeypatch.setattr(modulo_rmd, "_carregar_planilha_rmd", lambda: conteudo_excel)

    df = modulo_rmd.rmd(aba="2.1")
    assert df.columns == [
        "periodo",
        "detentor",
        "tipo",
        "categoria",
        "titulo",
        "valor",
    ]

    df_mar_26 = df.filter(pl.col("periodo") == pl.date(2026, 3, 1))

    total_publico = df_mar_26.filter(pl.col("detentor") == "Público")["valor"].sum()

    lft_tn = df_mar_26.filter(
        (pl.col("detentor") == "Público")
        & (pl.col("tipo") == "DPMFi")
        & (pl.col("categoria") == "Tesouro Nacional")
        & (pl.col("titulo") == "LFT")
    )["valor"].item()

    global_usd = df_mar_26.filter(
        (pl.col("detentor") == "Público")
        & (pl.col("tipo") == "DPFe")
        & (pl.col("categoria") == "Mobiliária")
        & (pl.col("titulo") == "Global USD")
    )["valor"].item()

    bc_total = df_mar_26.filter(pl.col("detentor") == "Banco Central")["valor"].sum()

    assert round(total_publico, 2) == TOTAL_PUBLICO_MAR_26
    assert lft_tn == LFT_TN_MAR_26
    assert global_usd == GLOBAL_USD_MAR_26
    assert round(bc_total, 2) == BC_TOTAL_MAR_26
