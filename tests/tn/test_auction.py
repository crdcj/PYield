import datetime as dt
import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import polars.selectors as cs

import pyield.tn.auctions as ac

DATA_DIR = Path(__file__).parent / "data"
JSON_PATH = DATA_DIR / "auction_20251023.json"
PARQUET_PATH = DATA_DIR / "auction_20251023.parquet"

# PTAX dos dias 22, 23 e 24/10/2025 usada na geração do Parquet de referência
PTAX_DF = pl.DataFrame(
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


def _load_json() -> list[dict]:
    return json.loads(JSON_PATH.read_bytes())


def _load_expected() -> pl.DataFrame:
    return pl.read_parquet(PARQUET_PATH)


def _run_pipeline(raw_data: list[dict], ptax_df: pl.DataFrame) -> pl.DataFrame:
    """Executa o pipeline de processamento completo sem acesso à rede."""
    df = ac._transformar_dados_brutos(raw_data)
    df = ac._adicionar_duration(df)
    df = ac._adicionar_dv01(df)
    with patch("pyield.tn.auctions._buscar_ptax", return_value=ptax_df):
        df = ac._adicionar_dv01_usd(df)
    df = ac._adicionar_prazo_medio(df)
    df = df.with_columns(cs.float().fill_nan(None))
    df = ac._selecionar_e_ordenar_colunas(df)
    return df


def test_process_pipeline():
    """Pipeline local deve produzir resultado idêntico ao Parquet de referência."""
    result = _run_pipeline(_load_json(), PTAX_DF)
    assert result.equals(_load_expected())


def test_auction_with_mock():
    """auction() com mocks de rede deve produzir o Parquet de referência."""
    with (
        patch("pyield.tn.auctions._buscar_dados_leilao", return_value=_load_json()),
        patch("pyield.tn.auctions._buscar_ptax", return_value=PTAX_DF),
    ):
        result = ac.auction(auction_date="23-10-2025")
    assert result.equals(_load_expected())
