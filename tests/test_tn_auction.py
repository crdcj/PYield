# tests/test_daily_dataframe.py
from datetime import date
from pathlib import Path

import polars as pl

from pyield.tn import auction

SNAPSHOT_DIR = Path(__file__).parent / "data" / "auction_23-10-2025.parquet"


def test_dataframe_matches_snapshot():
    test_date = date(2025, 10, 23)

    # Carregar snapshot
    expected = pl.read_parquet(SNAPSHOT_DIR)

    # Executar função
    result = auction(auction_date=test_date)

    # Comparar - tipos e valores
    assert result.equals(expected)
