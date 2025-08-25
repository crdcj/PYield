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
        "filename": "b3_di.parquet",
        "date_column": "TradeDate",
        "name": "Futuro de DI (B3)",
    },
    "tpf": {
        "filename": "anbima_tpf.parquet",
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
    """
    Função interna e cacheada. Sua única responsabilidade é carregar os dados
    e colocá-los no cache.
    """
    if dataset_id not in DATASET_CONFIGS:
        raise ValueError(f"Dataset com ID '{dataset_id}' não encontrado.")

    config = DATASET_CONFIGS[dataset_id]
    filename = config["filename"]
    full_url = f"{BASE_DATA_URL}/{filename}"

    try:
        # Apenas carrega e retorna. O .copy() aqui é opcional, mas inofensivo.
        df = _load_github_file(full_url)
        return df
    except Exception:
        logger.exception(f"Erro ao carregar dataset {dataset_id}")
        raise


def get_cached_dataset(dataset_id: Literal["di1", "tpf"]) -> pd.DataFrame:
    """
    Obtém um dataset configurado pelo seu ID, garantindo que o cache
    não seja modificado pelo chamador.
    O cache expira diariamente.
    """
    # 1. Pega o DataFrame do cache (ou aciona o carregamento)
    df_from_cache = _get_dataset_with_ttl(dataset_id.lower(), _get_today_date_key())

    # 2. Retorna uma CÓPIA para o usuário. Este é o passo crucial de proteção.
    return df_from_cache.copy()
