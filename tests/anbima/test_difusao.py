import datetime as dt
from pathlib import Path

import polars as pl

import pyield.anbima.difusao as difusao_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "difusao_20260205.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "difusao_20260205.parquet"
DATA_REFERENCIA = dt.date(2026, 2, 5)


def test_tpf_difusao_com_monkeypatch(monkeypatch):
    """tpf_difusao com monkeypatch deve bater com o parquet de referência."""
    monkeypatch.setattr(
        difusao_mod,
        "_fetch_url_data",
        lambda _: CAMINHO_CSV.read_text(encoding="utf-8"),
    )
    resultado = difusao_mod.tpf_difusao(DATA_REFERENCIA)
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
