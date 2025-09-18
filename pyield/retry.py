"""
Módulo de retry configurável para chamadas de API (usando requests) e
processamento de dados.

Este módulo fornece um decorador `tenacity.retry` pré-configurado (`default_retry`)
para lidar com erros transitórios.
"""

import logging

# Dependências de terceiros
import pandas as pd

# MUDANÇA CRUCIAL: Importar exceções da biblioteca 'requests'
from requests.exceptions import ConnectionError, HTTPError, Timeout
from tenacity import (
    RetryCallState,
    retry,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
)

# --- Configuração do Logger ---
logger = logging.getLogger(__name__)


# --- Funções Auxiliares para Tenacity ---
def _log_before_sleep(retry_state: RetryCallState):
    """Loga uma mensagem ANTES de o Tenacity entrar em espera entre tentativas."""
    outcome = retry_state.outcome
    if not outcome or not outcome.failed:
        return

    exception = outcome.exception()
    if not exception:
        return

    next_action = retry_state.next_action
    if not next_action or not hasattr(next_action, "sleep"):
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

    if isinstance(exception, HTTPError):
        # Acessa o status code através do objeto 'response'
        status_code = exception.response.status_code

        # CASOS PERMANENTES (NÃO TENTAR NOVAMENTE)
        # Erros 4xx do cliente, exceto 429 (Too Many Requests)
        if 400 <= status_code < 500 and status_code != 429:  # noqa
            return False

        # CASOS TRANSITÓRIOS (TENTAR NOVAMENTE)
        # Erro 429 (Too Many Requests) ou erros 5xx (Server Error)
        if status_code == 429 or status_code >= 500:  # noqa
            return True

    # Erros de rede genéricos que valem a pena tentar de novo
    if isinstance(exception, (Timeout, ConnectionError)):
        return True

    # Erros de parsing que podem ser de um download corrompido
    if isinstance(exception, (pd.errors.EmptyDataError, pd.errors.ParserError)):
        return True

    # Para qualquer outra exceção não listada, não tentar novamente.
    return False


# --- Decorador Tenacity Principal ---
default_retry = retry(
    retry=should_retry_exception,
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=(stop_after_attempt(3) | stop_after_delay(15)),
    before_sleep=_log_before_sleep,
    reraise=True,
)
