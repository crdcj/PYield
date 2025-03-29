"""
Módulo para carregamento e cache de dados financeiros.
Fornece acesso a dados DI e TPF com atualizações automáticas.
"""

import logging
import threading
from typing import Any, Dict
from zoneinfo import ZoneInfo

import pandas as pd

from pyield import bday

# Configuração de logging
logger = logging.getLogger(__name__)

# Configurações
TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")

GIT_URL = "https://raw.githubusercontent.com/crdcj/pyield-data/main"
DI_URL = f"{GIT_URL}/data/b3_di.parquet"
TPF_URL = f"{GIT_URL}/data/anbima_tpf.parquet"

# Configuração de datasets
DATASETS_CONFIG = {
    "di": {"url": f"{DI_URL}", "date_column": "TradeDate"},
    "tpf": {"url": f"{TPF_URL}", "date_column": "ReferenceDate"},
}


class DataCache:
    """Classe para gerenciar o cache dos datasets DI e TPF."""

    def __init__(self):
        """Inicializa o cache de dados vazio."""
        self._datasets: Dict[str, Dict[str, Any]] = {}
        self._locks: Dict[str, threading.Lock] = {}  # Trava por dataset

        # Inicializar configurações de datasets
        for key, config in DATASETS_CONFIG.items():
            self._datasets[key] = {
                "url": config["url"],
                "date_column": config["date_column"],
                "df": None,
                "last_date": None,
            }
            # Inicializa uma trava para cada dataset
            self._locks[key] = threading.Lock()

    def _should_update_dataset(self, key: str) -> bool:
        """
        Verifica se o dataset precisa ser atualizado.

        Args:
            key: Identificador do dataset

        Returns:
            True se o dataset precisar ser atualizado, False caso contrário
        """
        data_info = self._datasets[key]
        last_date_in_dataset = data_info["last_date"]

        # Se nunca foi carregado, deve atualizar
        if last_date_in_dataset is None:
            return True

        last_bday_in_bz = bday.last_business_day().date()
        n_bdays = bday.count(last_date_in_dataset, last_bday_in_bz)

        # Atualizar se houver mais de 1 dia útil de diferença
        return n_bdays > 1  # noqa

    def _load_dataset(self, key: str) -> None:
        """
        Carrega ou atualiza um dataset.

        Args:
            key: Identificador do dataset

        Raises:
            ValueError: Se o dataset não estiver configurado
            Exception: Se ocorrer erro ao carregar os dados
        """
        dataset_info = self._datasets[key]

        try:
            logger.info(f"Carregando dataset {key} de {dataset_info['url']}")
            df = pd.read_parquet(dataset_info["url"])

            # Validar se o DataFrame tem a coluna de data esperada
            date_column = dataset_info["date_column"]
            if date_column not in df.columns:
                raise ValueError(f"Coluna '{date_column}' não encontrada em '{key}'")

            dataset_info["df"] = df
            dataset_info["last_date"] = df[date_column].max().date()

            logger.info(
                f"Dataset {key} atualizado. Última data: {dataset_info['last_date']}"
            )

        except Exception as e:
            logger.error(f"Erro ao carregar/processar dataset {key}: {str(e)}")
            # Considere invalidar o cache em caso de erro
            dataset_info["df"] = None
            dataset_info["last_date"] = None
            raise  # Re-lança para o chamador saber do erro

    def get_dataframe(self, key: str) -> pd.DataFrame:
        """
        Obtém um DataFrame, carregando-o ou atualizando-o se necessário.

        Args:
            key: Identificador do dataset

        Returns:
            DataFrame com os dados solicitados

        Raises:
            ValueError: Se o dataset não estiver configurado
            Exception: Se ocorrer erro ao carregar os dados
        """
        dataset_info = self._datasets.get(key)

        if dataset_info is None:
            logger.error(f"Dataset '{key}' não está configurado")
            raise ValueError(f"Dataset '{key}' não está configurado")

        lock = self._locks[key]
        with lock:  # Garante thread safety
            try:
                # Verifica se precisa carregar/atualizar DENTRO do lock
                if dataset_info["df"] is None or self._should_update_dataset(key):
                    self._load_dataset(key)
                # Retorna cópia DENTRO do lock para segurança
                # (ou fora se a cópia for *muito* lenta e o risco for aceitável)
                if dataset_info["df"] is not None:
                    return dataset_info["df"].copy()
                else:
                    # Se _load_dataset falhou e deixou df como None
                    msg = f"Dataset {key} é None após tentativa de carregamento."
                    logger.error(msg)
                    # O erro original de _load_dataset (ou outro) será propagado
                    raise RuntimeError(msg)

            except Exception as e:
                logger.error(f"Erro final ao tentar obter dataframe {key}: {str(e)}")
                # O erro original de _load_dataset (ou outro) será propagado
                raise


def get_di_dataset(cache: DataCache) -> pd.DataFrame:
    """
    Obtém o dataset de DI (Depósito Interfinanceiro).

    Returns:
        DataFrame com os dados de DI

    Raises:
        Exception: Se ocorrer erro ao carregar os dados
    """
    return cache.get_dataframe("di")


def get_tpf_dataset(cache: DataCache) -> pd.DataFrame:
    """
    Obtém o dataset de TPF (Títulos Públicos Federais).

    Returns:
        DataFrame com os dados de TPF

    Raises:
        Exception: Se ocorrer erro ao carregar os dados
    """
    return cache.get_dataframe("tpf")
