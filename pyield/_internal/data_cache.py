import functools
from enum import Enum
from typing import Literal

import polars as pl
import requests

from pyield._internal.retry import requisitar
from pyield.relogio import agora

URL_BASE = "https://github.com/crdcj/pyield-data/releases/latest/download"


# Estrutura interna única — usuário não vê isso
class _Dataset(Enum):
    TPF = "anbima_tpf.parquet"
    FUTURO = "b3_futures.parquet"


type IdDataset = Literal["tpf", "futuro"]


def _obter_chave_data_hoje() -> str:
    return agora().strftime("%Y-%m-%d")


def _validar_id_dataset(id_dataset: str) -> _Dataset:
    try:
        return _Dataset[id_dataset.upper()]
    except KeyError as e:
        msg = f"id_dataset inválido: '{id_dataset}'. Valores aceitos: 'tpf', 'futuro'."
        raise ValueError(msg) from e


def _carregar_arquivo_github(url_arquivo: str) -> pl.DataFrame:
    """
    Baixa o arquivo usando requests e lê com Polars.
    Isso evita erros de 'object-store' em ambientes com proxies/firewalls
    sem precisar da dependência pesada do PyArrow.
    """
    # 1. Baixa os bytes usando requests (já lida com redirects e proxies do sistema)
    # Adicionando timeout para não travar o processo indefinidamente
    response = requisitar(requests.get, url_arquivo, timeout_leitura=10)

    # 2. O Polars lê o buffer como se fosse um arquivo local
    return pl.read_parquet(response.content)


@functools.lru_cache(maxsize=8)
def _obter_dataset_com_ttl(id_dataset: str, chave_data: str) -> pl.DataFrame:
    _ = chave_data
    config = _validar_id_dataset(id_dataset)
    return _carregar_arquivo_github(f"{URL_BASE}/{config.value}")


def obter_dataset_cacheado(id_dataset: IdDataset) -> pl.DataFrame:
    """
    Obtém um dataset pelo ID. Cache expira diariamente.

    Args:
        id_dataset: "tpf" ou "futuro"
    """
    df = _obter_dataset_com_ttl(id_dataset.lower(), _obter_chave_data_hoje())
    return df.clone()
