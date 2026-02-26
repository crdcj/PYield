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


def test_tpf_difusao_com_monkeypatch(monkeypatch):
    """tpf_difusao com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        difusao_mod,
        "_buscar_dados_url",
        lambda _: CAMINHO_CSV.read_text(encoding="utf-8"),
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


def test_tpf_difusao_propaga_timeout(monkeypatch):
    """tpf_difusao deve propagar erro operacional de rede."""

    def _mock_busca(_data: str) -> str:
        raise requests.Timeout("timeout simulado")

    monkeypatch.setattr(difusao_mod, "_buscar_dados_url", _mock_busca)

    with pytest.raises(requests.Timeout):
        difusao_mod.tpf_difusao(DATA_REFERENCIA)


def test_tpf_difusao_falha_com_csv_malformado(monkeypatch):
    """tpf_difusao deve falhar para CSV malformado."""
    monkeypatch.setattr(
        difusao_mod, "_buscar_dados_url", lambda _: "cabecalho invalido"
    )

    with pytest.raises(ValueError, match="Cabeçalho do CSV inválido"):
        difusao_mod.tpf_difusao(DATA_REFERENCIA)
