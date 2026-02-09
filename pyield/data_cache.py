import functools
import io
import logging
from enum import Enum
from typing import Literal

import polars as pl
import requests

from pyield.clock import now
from pyield.retry import default_retry

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


def _validar_dataset_id(dataset_id: str) -> _Dataset:
    dataset_normalizado = dataset_id.lower()
    try:
        return _Dataset[dataset_normalizado.upper()]
    except KeyError as e:
        msg = f"dataset_id inválido: '{dataset_id}'. Valores aceitos: 'di1', 'tpf'."
        raise ValueError(msg) from e


@default_retry
def _load_github_file(file_url: str) -> pl.DataFrame:
    """
    Baixa o arquivo usando requests e lê com Polars.
    Isso evita erros de 'object-store' em ambientes com proxies/firewalls
    sem precisar da dependência pesada do PyArrow.
    """
    # 1. Baixa os bytes usando requests (já lida com redirects e proxies do sistema)
    # Adicionando timeout para não travar o processo indefinidamente
    response = requests.get(file_url, timeout=10)

    # Garante que a requisição foi sucesso (200 OK), senão levanta erro
    response.raise_for_status()

    # 2. Transforma os bytes em um objeto de arquivo em memória
    file_buffer = io.BytesIO(response.content)

    # 3. O Polars lê o buffer como se fosse um arquivo local
    return pl.read_parquet(file_buffer)


@functools.lru_cache(maxsize=8)
def _get_dataset_with_ttl(dataset_id: str, date_key: str) -> pl.DataFrame:
    config = _validar_dataset_id(dataset_id)
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
