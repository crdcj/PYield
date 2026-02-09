import datetime as dt
import json
from pathlib import Path

import polars as pl

import pyield.tn.auctions as auctions_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_JSON = DIRETORIO_DADOS / "auction_20251023.json"
CAMINHO_PARQUET = DIRETORIO_DADOS / "auction_20251023.parquet"

# PTAX dos dias 22, 23 e 24/10/2025 usada na geração do Parquet de referência
DF_PTAX = pl.DataFrame(
    {
        "data_ref": [
            dt.date(2025, 10, 22),
            dt.date(2025, 10, 23),
            dt.date(2025, 10, 24),
        ],
        "ptax": [5.3895, 5.3837, 5.3794],
    },
    schema={"data_ref": pl.Date, "ptax": pl.Float64},
)


def test_auction_com_monkeypatch(monkeypatch):
    """auction com monkeypatch deve produzir o Parquet de referência."""
    monkeypatch.setattr(
        auctions_mod,
        "_buscar_dados_leilao",
        lambda *_, **__: json.loads(CAMINHO_JSON.read_bytes()),
    )
    monkeypatch.setattr(auctions_mod, "_buscar_ptax", lambda *_, **__: DF_PTAX)
    resultado = auctions_mod.auction(auction_date="23-10-2025")
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
