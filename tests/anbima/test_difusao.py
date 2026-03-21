import datetime as dt
from pathlib import Path

import polars as pl
import pytest
import requests

import pyield.anbima.difusao as difusao_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "difusao_20260205.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "difusao_20260205.parquet"
DATA_REFERENCIA = dt.date(2026, 2, 5)

CAMINHO_CSV_SELIC = DIRETORIO_DADOS / "difusao_20230320.csv"
CAMINHO_PARQUET_SELIC = DIRETORIO_DADOS / "difusao_20230320.parquet"
DATA_REFERENCIA_SELIC = dt.date(2023, 3, 20)


def test_tpf_difusao_com_monkeypatch(monkeypatch):
    """tpf_difusao com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        difusao_mod,
        "_buscar_dados_url",
        lambda _: CAMINHO_CSV.read_bytes(),
    )
    resultado = difusao_mod.tpf_difusao(DATA_REFERENCIA)
    esperado = pl.read_parquet(CAMINHO_PARQUET).with_columns(
        pl.col("data_hora_referencia").dt.replace_time_zone("America/Sao_Paulo")
    )

    assert resultado.equals(esperado)
    assert resultado.schema["data_hora_referencia"] == pl.Datetime(
        time_zone="America/Sao_Paulo"
    )

    resultado_12h = resultado.filter(pl.col("provedor") == "ANBIMA 12H")
    assert resultado_12h.height > 0
    assert resultado_12h["data_hora_referencia"].dt.hour().unique().to_list() == [12]


def test_tpf_difusao_com_linhas_selic(monkeypatch):
    """tpf_difusao com CSV contendo linhas SELIC (separador de milhar)."""
    monkeypatch.setattr(
        difusao_mod,
        "_buscar_dados_url",
        lambda _: CAMINHO_CSV_SELIC.read_bytes(),
    )
    resultado = difusao_mod.tpf_difusao(DATA_REFERENCIA_SELIC)
    esperado = pl.read_parquet(CAMINHO_PARQUET_SELIC).with_columns(
        pl.col("data_hora_referencia").dt.replace_time_zone("America/Sao_Paulo")
    )

    assert resultado.equals(esperado)


def test_tpf_difusao_propaga_timeout(monkeypatch):
    """tpf_difusao deve propagar erro operacional de rede."""

    def _mock_busca(_data: str) -> str:
        raise requests.Timeout("timeout simulado")

    monkeypatch.setattr(difusao_mod, "_buscar_dados_url", _mock_busca)

    with pytest.raises(requests.Timeout):
        difusao_mod.tpf_difusao(DATA_REFERENCIA)
