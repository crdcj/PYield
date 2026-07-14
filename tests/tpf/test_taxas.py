import datetime as dt
import importlib
import zipfile as zf
from pathlib import Path

import polars as pl

modulo_mercado_secundario = importlib.import_module("pyield.anbima.mercado_secundario")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "tpf_20260206.txt"
CAMINHO_PARQUET = DIRETORIO_DADOS / "tpf_20260206.parquet"


def test_pipeline_taxas():
    """tpf.taxas(): pipeline de processamento do CSV bruto deve bater com o parquet de referência."""
    csv_bruto = CAMINHO_CSV.read_bytes()
    df_resultado = modulo_mercado_secundario._parsear_df(csv_bruto)
    df_resultado = modulo_mercado_secundario._processar_df(df_resultado)

    assert df_resultado.equals(pl.read_parquet(CAMINHO_PARQUET))


def test_ler_arquivo():
    df_arquivo = modulo_mercado_secundario.ler_arquivo(CAMINHO_CSV)
    df_processado = modulo_mercado_secundario._processar_df(
        modulo_mercado_secundario._parsear_df(CAMINHO_CSV.read_bytes())
    )

    assert df_arquivo.equals(df_processado)


def test_ler_arquivo_exe(tmp_path):
    caminho = tmp_path / "ms140512.exe"
    with zf.ZipFile(caminho, "w") as arquivo_zip:
        arquivo_zip.writestr("ms140512.txt", CAMINHO_CSV.read_bytes())

    df_arquivo = modulo_mercado_secundario.ler_arquivo(caminho)
    df_processado = modulo_mercado_secundario._processar_df(
        modulo_mercado_secundario._parsear_df(CAMINHO_CSV.read_bytes())
    )

    assert df_arquivo.equals(df_processado)


def test_baixar_arquivo(monkeypatch):
    data_recebida = None
    conteudo = b"conteudo bruto"

    def obter_csv(data):
        nonlocal data_recebida
        data_recebida = data
        return conteudo

    monkeypatch.setattr(modulo_mercado_secundario, "_obter_csv", obter_csv)

    resultado = modulo_mercado_secundario.baixar_arquivo("06-02-2026")

    assert resultado == conteudo
    assert data_recebida == dt.date(2026, 2, 6)
