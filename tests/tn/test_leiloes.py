import datetime as dt
import importlib
import json
from pathlib import Path

import polars as pl

modulo_leiloes = importlib.import_module("pyield.tn.leiloes")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_JSON = DIRETORIO_DADOS / "leilao_20251023.json"
CAMINHO_PARQUET = DIRETORIO_DADOS / "leilao_20251023.parquet"

# PTAX dos dias 22, 23 e 24/10/2025 usada na geração do Parquet de referência
DF_PTAX_REFERENCIA = pl.DataFrame(
    {
        "data_ref": [
            dt.date(2025, 10, 22),
            dt.date(2025, 10, 23),
            dt.date(2025, 10, 24),
        ],
        "ptax": [5.3898, 5.384, 5.3797],
    },
    schema={"data_ref": pl.Date, "ptax": pl.Float64},
)


def test_leilao_com_monkeypatch(monkeypatch):
    """leilao_tn com monkeypatch deve produzir o Parquet de referência."""
    monkeypatch.setattr(
        modulo_leiloes,
        "_buscar_dados_leilao",
        lambda *_, **__: json.loads(CAMINHO_JSON.read_bytes()),
    )
    monkeypatch.setattr(
        modulo_leiloes,
        "_buscar_ptax",
        lambda *_, **__: DF_PTAX_REFERENCIA,
    )
    resultado = modulo_leiloes.leilao(data="23-10-2025")
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
