"""
Módulo para carregamento e cache de dados financeiros.
Fornece acesso a dados DI e TPF com atualizações automáticas.
"""

import logging
from functools import wraps
from typing import Any, Dict
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pyield import bday

# Configuração de logging
logger = logging.getLogger(__name__)

# Configurações
TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")

GIT_URL = "https://raw.githubusercontent.com/crdcj/pyield-data/main"
DI_URL = f"{GIT_URL}/data/b3_di.pkl.gz"
TPF_URL = f"{GIT_URL}/data/anbima_tpf.pkl.gz"

# Configuração de datasets
DATASETS_CONFIG = {
    "di": {"url": f"{DI_URL}", "date_column": "TradeDate"},
    "tpf": {"url": f"{TPF_URL}", "date_column": "ReferenceDate"},
}


def singleton(cls):
    """Decorator para implementar o padrão Singleton."""
    instances = {}

    @wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


@singleton
class DataCache:
    """
    Classe para gerenciar o cache de datasets financeiros.
    Implementa o padrão Singleton para garantir uma única instância.
    """

    def __init__(self):
        """Inicializa o cache de dados vazio."""
        self._datasets: Dict[str, Dict[str, Any]] = {}

        # Inicializar configurações de datasets
        for key, config in DATASETS_CONFIG.items():
            self._datasets[key] = {
                "url": config["url"],
                "date_column": config["date_column"],
                "df": None,
                "last_date": None,
            }

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

        bz_last_bday = bday.last_business_day().date()
        n_bdays = bday.count(last_date_in_dataset, bz_last_bday)

        # Atualizar se houver mais de 1 dia útil de diferença
        return n_bdays > 1  # noqa

    def _load_dataset(self, key: str) -> None:
        """
        Carrega ou atualiza um dataset.

        Args:
            key: Identificador do dataset

        Raises:
            requests.RequestException: Se ocorrer erro ao fazer download dos dados
            ValueError: Se o formato dos dados for inválido
        """
        dataset_info = self._datasets[key]

        try:
            logger.info(f"Carregando dataset {key} de {dataset_info['url']}")
            df = pd.read_pickle(dataset_info["url"])

            # Validar se o DataFrame tem a coluna de data esperada
            date_column = dataset_info["date_column"]
            if date_column not in df.columns:
                raise ValueError(
                    f"Coluna de data '{date_column}' não encontrada no dataset '{key}'"
                )

            dataset_info["df"] = df
            dataset_info["last_date"] = df[date_column].max().date()

            logger.info(
                f"Dataset {key} atualizado. Última data: {dataset_info['last_date']}"
            )

        except requests.RequestException as e:
            logger.error(f"Erro ao fazer download do dataset {key}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Erro ao processar dataset {key}: {str(e)}")
            raise

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

        try:
            if dataset_info["df"] is None or self._should_update_dataset(key):
                self._load_dataset(key)

            return dataset_info["df"].copy()
        except Exception as e:
            logger.error(f"Erro ao obter dataset {key}: {str(e)}")
            raise e


# Instância única da classe para uso interno da biblioteca
_data_cache = DataCache()


def get_di_dataset() -> pd.DataFrame:
    """
    Obtém o dataset de DI (Depósito Interfinanceiro).

    Returns:
        DataFrame com os dados de DI

    Raises:
        Exception: Se ocorrer erro ao carregar os dados
    """
    return _data_cache.get_dataframe("di")


def get_tpf_dataset() -> pd.DataFrame:
    """
    Obtém o dataset de TPF (Títulos Públicos Federais).

    Returns:
        DataFrame com os dados de TPF

    Raises:
        Exception: Se ocorrer erro ao carregar os dados
    """
    return _data_cache.get_dataframe("tpf")
