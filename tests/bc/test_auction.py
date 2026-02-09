import datetime as dt
from pathlib import Path

import polars as pl

import pyield.bc.auction as auction_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "auction_20250819.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "auction_20250819.parquet"

# PTAX do dia 2025-08-19 usada na geração do Parquet de referência
DF_PTAX = pl.DataFrame(
    {"Date": [dt.date(2025, 8, 19)], "PTAX": [5.4713]},
    schema={"Date": pl.Date, "PTAX": pl.Float64},
)


def test_auctions_com_monkeypatch(monkeypatch):
    """auctions com monkeypatch deve produzir o Parquet de referência."""
    monkeypatch.setattr(
        auction_mod,
        "_buscar_csv_api",
        lambda *_: CAMINHO_CSV.read_bytes(),
    )
    monkeypatch.setattr(auction_mod, "_obter_df_ptax", lambda *_: DF_PTAX)
    resultado = auction_mod.auctions(start="19-08-2025", end="19-08-2025")
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
