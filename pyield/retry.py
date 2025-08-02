"""
Módulo de retry configurável para chamadas de API e processamento de dados.

Este módulo fornece um decorador `tenacity.retry` pré-configurado (`default_retry`)
para lidar com erros transitórios comuns ao interagir com APIs web e ao
processar os dados resultantes (ex: parsing de CSV/JSON).
"""

import logging
from urllib.error import HTTPError, URLError

# Dependências de terceiros
import pandas as pd
import tenacity

# Importar tipos específicos necessários
from tenacity import RetryCallState

# --- Configuração do Logger ---
logger = logging.getLogger(__name__)


# --- Funções Auxiliares para Tenacity ---
def _log_before_sleep(retry_state: RetryCallState):
    """
    Loga uma mensagem ANTES de o Tenacity entrar em espera (sleep) entre tentativas.
    """
    outcome = retry_state.outcome
    if not outcome or not outcome.failed:
        logger.debug(
            "before_sleep chamada sem 'outcome' ou para 'outcome' bem-sucedido."
        )
        return

    exception = outcome.exception()
    if not exception:
        logger.debug("before_sleep chamada sem 'exception' real no outcome.")
        return

    next_action = retry_state.next_action
    if not next_action or not hasattr(next_action, "sleep"):
        logger.debug(
            "before_sleep chamada sem 'next_action' ou 'sleep' em next_action."
        )
        return

    sleep_duration = next_action.sleep

    logger.warning(
        f"Tentativa {retry_state.attempt_number} falhou com "
        f"{type(exception).__name__}: {str(exception)[:150]}. Tentando novamente em "
        f"{sleep_duration:.2f} segundos..."
    )


def should_retry_exception(retry_state: RetryCallState) -> bool:  # noqa
    """Determina se uma exceção capturada justifica uma nova tentativa."""
    if not retry_state.outcome or not retry_state.outcome.failed:
        return False

    exception = retry_state.outcome.exception()
    if exception is None:
        return False

    # A decisão de retentativa pode ser muito mais concisa
    match exception:
        # CASOS ONDE NÃO DEVEMOS TENTAR NOVAMENTE
        case HTTPError(code=404):
            return False  # Erro permanente
        case HTTPError(code=504) if retry_state.attempt_number > 1:
            return False  # Já tentamos uma vez para Gateway Timeout

        # CASOS ONDE DEVEMOS TENTAR NOVAMENTE
        case HTTPError(code=429):
            return True  # Too Many Requests, vale a pena esperar
        case HTTPError() if getattr(exception, "code", 0) >= 500:  # noqa
            return True  # Erros de servidor (500, 502, 503, 504 na 1a vez)
        case URLError():
            return True  # Erros de rede genéricos
        case pd.errors.EmptyDataError() | pd.errors.ParserError():
            return True  # Talvez o arquivo foi baixado corrompido, tente de novo

        # CASO PADRÃO: Erros inesperados ou erros de cliente (4xx) não especificados
        case _:
            return False


# --- Decorador Tenacity Principal ---
default_retry = tenacity.retry(
    retry=should_retry_exception,
    # Backoff exponencial é bom para 429
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    stop=(tenacity.stop_after_attempt(3) | tenacity.stop_after_delay(15)),
    before_sleep=_log_before_sleep,
    reraise=True,  # Importante para propagar o erro final se as tentativas falharem
)
