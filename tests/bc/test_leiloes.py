import datetime as dt
import importlib
from pathlib import Path

import polars as pl

leiloes_mod = importlib.import_module("pyield.bc.leiloes")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "auction_20250819.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "auction_20250819.parquet"

# PTAX do dia 2025-08-19 usada na geração do Parquet de referência
DF_PTAX = pl.DataFrame(
    {"data_ref": [dt.date(2025, 8, 19)], "ptax": [5.4713]},
    schema={"data_ref": pl.Date, "ptax": pl.Float64},
)


def test_leiloes_com_monkeypatch(monkeypatch):
    """leiloes com monkeypatch deve produzir o Parquet de referência."""
    monkeypatch.setattr(
        leiloes_mod,
        "_buscar_csv",
        lambda *_: CAMINHO_CSV.read_bytes(),
    )
    monkeypatch.setattr(leiloes_mod, "_buscar_ptax", lambda *_: DF_PTAX)
    resultado = leiloes_mod.leiloes(
        data_inicial="19-08-2025",
        data_final="19-08-2025",
    )
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))
