import datetime as dt
from pathlib import Path

import polars as pl
import pytest

import pyield.anbima.difusao as difusao_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"

CASOS = [
    (dt.date(2026, 2, 5), "difusao_20260205"),
    (dt.date(2023, 3, 20), "difusao_20230320"),
]


@pytest.mark.parametrize(("data_ref", "nome_base"), CASOS)
def test_tpf_difusao(monkeypatch, data_ref, nome_base):
    """Pipeline CSV → DataFrame deve bater com o parquet de referência."""
    csv = DIRETORIO_DADOS / f"{nome_base}.csv"
    parquet = DIRETORIO_DADOS / f"{nome_base}.parquet"
    monkeypatch.setattr(difusao_mod, "_buscar_dados_url", lambda _: csv.read_bytes())
    resultado = difusao_mod.tpf_difusao(data_ref)
    esperado = pl.read_parquet(parquet)

    assert resultado.equals(esperado)
