import functools
import logging
from datetime import datetime
from typing import Literal
from zoneinfo import ZoneInfo

import pandas as pd

from pyield.retry import default_retry

# --- Configurações Centralizadas ---
TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")

GIT_URL = "https://raw.githubusercontent.com/crdcj/pyield-data/main"
# Diretório base onde os arquivos parquet estão localizados
BASE_DATA_URL = f"{GIT_URL}/data"

# Dicionário global para configurações dos datasets usando 'filename'
DATASET_CONFIGS = {
    "di1": {
        "filename": "b3_di.parquet",  # Apenas o nome do arquivo
        "date_column": "TradeDate",
        "name": "Futuro de DI (B3)",
    },
    "tpf": {
        "filename": "anbima_tpf.parquet",  # Apenas o nome do arquivo
        "date_column": "ReferenceDate",
        "name": "TPF (ANBIMA)",
    },
    # --- Adicionar futuros datasets aqui ---
    # "NOVODATASET": {
    #     "filename": "novo_dataset.parquet",
    #     "date_column": "DataReferencia",
    #     "name": "Novo Dataset Exemplo"
    # },
}

logger = logging.getLogger(__name__)


def _get_today_date_key():
    """Retorna a data atual como string no formato YYYY-MM-DD."""
    return datetime.now(TIMEZONE_BZ).strftime("%Y-%m-%d")


@default_retry
def _load_github_file(file_url: str) -> pd.DataFrame:
    """Carrega um arquivo do GitHub e retorna um DataFrame."""
    return pd.read_parquet(file_url)


@functools.lru_cache(maxsize=len(DATASET_CONFIGS))
def _get_dataset_with_ttl(dataset_id: str, date_key: str) -> pd.DataFrame:
    """Carrega o dataset a partir do ID e da chave de data. Cache expira diariamente."""
    if dataset_id not in DATASET_CONFIGS:
        raise ValueError(f"Dataset com ID '{dataset_id}' não encontrado.")

    config = DATASET_CONFIGS[dataset_id]
    filename = config["filename"]
    date_column = config["date_column"]
    dataset_name = config.get("name", dataset_id)

    # Constrói a URL completa
    full_url = f"{BASE_DATA_URL}/{filename}"

    try:
        df = _load_github_file(full_url)
        last_date = df[date_column].max().date()
        logger.info(f"Dataset {dataset_name} carregado. Última data: {last_date}")
        return df.copy()

    except Exception:
        logger.exception(f"Erro ao carregar dataset {dataset_name} ({dataset_id})")
        raise  # Re-raise a exceção para que o chamador saiba que falhou


def get_cached_dataset(dataset_id: Literal["di1", "tpf"]) -> pd.DataFrame:
    """Obtém um dataset configurado pelo seu ID. O cache expira diariamente."""
    return _get_dataset_with_ttl(dataset_id.lower(), _get_today_date_key())
