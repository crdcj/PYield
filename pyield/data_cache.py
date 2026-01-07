import functools
import logging
from dataclasses import dataclass
from typing import Literal

import polars as pl

from pyield.clock import now

BASE_URL = "https://github.com/crdcj/pyield-data/releases/latest/download"


# Usar um dataclass para uma configuração mais estruturada e segura.
@dataclass(frozen=True)
class DatasetConfig:
    filename: str
    date_column: str
    name: str


# Dicionário global para configurações dos datasets
DATASET_CONFIGS: dict[str, DatasetConfig] = {
    "di1": DatasetConfig(
        filename="b3_di.parquet",
        date_column="TradeDate",
        name="Futuro de DI (B3)",
    ),
    "tpf": DatasetConfig(
        filename="anbima_tpf.parquet",
        date_column="ReferenceDate",
        name="TPF (ANBIMA)",
    ),
}

# Gerar o tipo Literal dinamicamente a partir das chaves do dicionário.
DatasetId = Literal[*DATASET_CONFIGS.keys()]

logger = logging.getLogger(__name__)


def _get_today_date_key() -> str:
    """Retorna a data atual como string no formato YYYY-MM-DD."""
    return now().strftime("%Y-%m-%d")


def _load_github_file(file_url: str) -> pl.DataFrame:
    """Carrega um arquivo do GitHub de forma robusta e retorna um DataFrame."""
    return pl.read_parquet(file_url, use_pyarrow=True)


@functools.lru_cache(maxsize=len(DATASET_CONFIGS))
def _get_dataset_with_ttl(dataset_id: str, date_key: str) -> pl.DataFrame:
    """
    Função interna que carrega dados do GitHub. É cacheada por `lru_cache`.

    O argumento `date_key` não é usado no corpo da função, mas é essencial
    para o mecanismo de cache. O `lru_cache` o usa como parte da chave,
    garantindo que o cache seja invalidado quando o dia muda.
    """
    if dataset_id not in DATASET_CONFIGS:
        raise ValueError(f"Dataset com ID '{dataset_id}' não encontrado.")

    config = DATASET_CONFIGS[dataset_id]
    # Acesso via atributo com dataclass
    full_url = f"{BASE_URL}/{config.filename}"

    try:
        return _load_github_file(full_url)
    except Exception:
        logger.exception(f"Erro ao carregar o dataset '{dataset_id}' da URL {full_url}")
        raise


def get_cached_dataset(dataset_id: DatasetId) -> pl.DataFrame:
    """
    Obtém um dataset configurado pelo seu ID, garantindo que o cache
    não seja modificado pelo chamador. O cache expira diariamente.
    """
    # 1. Pega o DataFrame do cache (ou aciona o carregamento)
    #    O .lower() garante que a chamada seja case-insensitive.
    df_from_cache = _get_dataset_with_ttl(dataset_id.lower(), _get_today_date_key())

    # 2. Retorna uma CÓPIA para o usuário para proteger o cache.
    return df_from_cache.clone()
