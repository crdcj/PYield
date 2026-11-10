import functools
from typing import Literal

import polars as pl
import requests

from pyield._internal.retry import requisitar
from pyield.relogio import agora

URL_BASE = "https://github.com/crdcj/pyield-data/releases/latest/download"


type IdDataset = Literal["tpf", "futuro"]

_ARQUIVOS: dict[IdDataset, str] = {
    "tpf": "anbima_tpf.parquet",
    "futuro": "b3_futures.parquet",
}


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
def _obter_dataset_com_ttl(id_dataset: IdDataset, chave_data: str) -> pl.DataFrame:
    # chave_data só compõe a chave do cache, que assim expira a cada dia.
    _ = chave_data
    return _carregar_arquivo_github(f"{URL_BASE}/{_ARQUIVOS[id_dataset]}")


def obter_dataset_cacheado(id_dataset: IdDataset) -> pl.DataFrame:
    """
    Obtém um dataset pelo ID. Cache expira diariamente.

    Args:
        id_dataset: "tpf" ou "futuro"
    """
    return _obter_dataset_com_ttl(id_dataset, f"{agora():%Y-%m-%d}").clone()
