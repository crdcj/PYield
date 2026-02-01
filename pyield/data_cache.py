import functools
import logging
from enum import Enum
from typing import Literal

import polars as pl

from pyield.clock import now

BASE_URL = "https://github.com/crdcj/pyield-data/releases/latest/download"
logger = logging.getLogger(__name__)


# Estrutura interna única — usuário não vê isso
class _Dataset(Enum):
    DI1 = ("b3_di.parquet", "TradeDate", "Futuro de DI (B3)")
    TPF = ("anbima_tpf.parquet", "ReferenceDate", "TPF (ANBIMA)")

    def __init__(self, filename: str, date_column: str, description: str):
        self.filename = filename
        self.date_column = date_column
        self.description = description


# API pública — só isso aparece pro usuário
type DatasetId = Literal["di1", "tpf"]


def _get_today_date_key() -> str:
    return now().strftime("%Y-%m-%d")


def _load_github_file(file_url: str) -> pl.DataFrame:
    return pl.read_parquet(file_url, use_pyarrow=True)


@functools.lru_cache(maxsize=8)
def _get_dataset_with_ttl(dataset_id: str, date_key: str) -> pl.DataFrame:
    config = _Dataset[dataset_id.upper()]
    full_url = f"{BASE_URL}/{config.filename}"
    try:
        return _load_github_file(full_url)
    except Exception:
        logger.exception(f"Erro ao carregar dataset '{dataset_id}' de {full_url}")
        raise


def get_cached_dataset(dataset_id: DatasetId) -> pl.DataFrame:
    """
    Obtém um dataset pelo ID. Cache expira diariamente.

    Args:
        dataset_id: "di1" ou "tpf"
    """
    df = _get_dataset_with_ttl(dataset_id.lower(), _get_today_date_key())
    return df.clone()
