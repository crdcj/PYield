import datetime as dt
from pathlib import Path

import polars as pl

from pyield.anbima import tpf

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "tpf_20260206.txt"
CAMINHO_PARQUET = DIRETORIO_DADOS / "tpf_20260206.parquet"
DATA_TESTE = dt.date(2026, 2, 6)


def test_process_pipeline(monkeypatch):
    """Pipeline de processamento deve bater com o parquet de referência."""
    monkeypatch.setattr(
        tpf,
        "_montar_url_arquivo",
        lambda _: f"{tpf.ANBIMA_URL}/{CAMINHO_CSV.name}",
    )
    monkeypatch.setattr(tpf, "_obter_csv", lambda _: CAMINHO_CSV.read_bytes())

    df = tpf.tpf_data(DATA_TESTE, fetch_from_source=True)

    assert df.equals(pl.read_parquet(CAMINHO_PARQUET))
