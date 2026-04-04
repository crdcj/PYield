import importlib
from pathlib import Path

import polars as pl

trades_mod = importlib.import_module("pyield.bc.tpf_intradiario")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "trades_intraday_20260206.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "trades_intraday_20260206.parquet"

# Colunas que dependem do horário de execução, não da lógica de processamento.
COLUNAS_IGNORAR = ["data_hora_consulta", "data_liquidacao"]


def test_trades_intraday_com_monkeypatch(monkeypatch):
    """tpf_intradiario com monkeypatch deve bater com o parquet."""
    monkeypatch.setattr(
        trades_mod,
        "_buscar_csv",
        lambda: CAMINHO_CSV.read_bytes(),
    )
    monkeypatch.setattr(trades_mod, "_mercado_selic_aberto", lambda: True)

    resultado = trades_mod.tpf_intradiario().drop(COLUNAS_IGNORAR)
    esperado = pl.read_parquet(CAMINHO_PARQUET).drop(COLUNAS_IGNORAR)
    assert resultado.equals(esperado)
