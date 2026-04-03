import functools
import logging
from enum import Enum
from typing import Literal

import polars as pl
import requests

from pyield._internal.retry import retry_padrao
from pyield.clock import now

URL_BASE = "https://github.com/crdcj/pyield-data/releases/latest/download"
registro = logging.getLogger(__name__)


# Estrutura interna única — usuário não vê isso
class _Dataset(Enum):
    TPF = ("anbima_tpf.parquet", "data_referencia", "TPF (ANBIMA)")
    FUTURES = ("b3_futures.parquet", "data_referencia", "Futures (B3)")

    def __init__(self, nome_arquivo: str, coluna_data: str, descricao: str):
        self.nome_arquivo = nome_arquivo
        self.coluna_data = coluna_data
        self.descricao = descricao


type IdDataset = Literal["tpf", "futures"]


def _obter_chave_data_hoje() -> str:
    return now().strftime("%Y-%m-%d")


def _validar_id_dataset(id_dataset: str) -> _Dataset:
    dataset_normalizado = id_dataset.lower()
    try:
        return _Dataset[dataset_normalizado.upper()]
    except KeyError as e:
        msg = f"id_dataset inválido: '{id_dataset}'. Valores aceitos: 'tpf', 'futures'."
        raise ValueError(msg) from e


@retry_padrao
def _carregar_arquivo_github(url_arquivo: str) -> pl.DataFrame:
    """
    Baixa o arquivo usando requests e lê com Polars.
    Isso evita erros de 'object-store' em ambientes com proxies/firewalls
    sem precisar da dependência pesada do PyArrow.
    """
    # 1. Baixa os bytes usando requests (já lida com redirects e proxies do sistema)
    # Adicionando timeout para não travar o processo indefinidamente
    response = requests.get(url_arquivo, timeout=10)

    # Garante que a requisição foi sucesso (200 OK), senão levanta erro
    response.raise_for_status()

    # 2. O Polars lê o buffer como se fosse um arquivo local
    return pl.read_parquet(response.content)


@functools.lru_cache(maxsize=8)
def _obter_dataset_com_ttl(id_dataset: str, chave_data: str) -> pl.DataFrame:
    _ = chave_data
    config = _validar_id_dataset(id_dataset)
    url_completa = f"{URL_BASE}/{config.nome_arquivo}"
    try:
        return _carregar_arquivo_github(url_completa)
    except Exception:
        registro.exception(
            "Erro ao carregar dataset '%s' de %s", id_dataset, url_completa
        )
        raise


def obter_dataset_cacheado(id_dataset: IdDataset) -> pl.DataFrame:
    """
    Obtém um dataset pelo ID. Cache expira diariamente.

    Args:
        id_dataset: "tpf" ou "futures"
    """
    df = _obter_dataset_com_ttl(id_dataset.lower(), _obter_chave_data_hoje())
    return df.clone()
