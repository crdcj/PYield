import functools
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

# Configurações
GIT_URL = "https://raw.githubusercontent.com/crdcj/pyield-data/main"
DI_URL = f"{GIT_URL}/data/b3_di.parquet"
TPF_URL = f"{GIT_URL}/data/anbima_tpf.parquet"

TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")
logger = logging.getLogger(__name__)


# Funções internas para controle de cache
def _get_today_date_key():
    """Retorna a data atual como string no formato YYYY-MM-DD."""
    return datetime.now(TIMEZONE_BZ).strftime("%Y-%m-%d")


@functools.lru_cache(maxsize=1)
def _get_di_dataset_with_ttl(date_key: str) -> pd.DataFrame:
    """Implementação interna com TTL para o dataset DI."""
    try:
        logger.info(f"Carregando dataset DI de {DI_URL}")
        df = pd.read_parquet(DI_URL)

        if "TradeDate" not in df.columns:
            raise ValueError("Coluna 'TradeDate' não encontrada no dataset DI")

        last_date = df["TradeDate"].max().date()
        logger.info(f"Dataset DI carregado. Última data: {last_date}")

        return df.copy()

    except Exception as e:
        logger.error(f"Erro ao carregar/processar dataset DI: {str(e)}")
        raise


@functools.lru_cache(maxsize=1)
def _get_tpf_dataset_with_ttl(date_key: str) -> pd.DataFrame:
    """Implementação interna com TTL para o dataset TPF."""
    try:
        logger.info(f"Carregando dataset TPF de {TPF_URL}")
        df = pd.read_parquet(TPF_URL)

        if "ReferenceDate" not in df.columns:
            raise ValueError("Coluna 'ReferenceDate' não encontrada no dataset TPF")

        last_date = df["ReferenceDate"].max().date()
        logger.info(f"Dataset TPF carregado. Última data: {last_date}")

        return df.copy()

    except Exception as e:
        logger.error(f"Erro ao carregar/processar dataset TPF: {str(e)}")
        raise


# Funções públicas com a interface original
def get_di_dataset() -> pd.DataFrame:
    """Obtém o dataset de Futuro de DI. O cache expira diariamente."""
    return _get_di_dataset_with_ttl(_get_today_date_key())


def get_tpf_dataset() -> pd.DataFrame:
    """Obtém o dataset de TPF. O cache expira diariamente."""
    return _get_tpf_dataset_with_ttl(_get_today_date_key())
