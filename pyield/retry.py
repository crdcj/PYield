"""
Módulo de retry configurável para chamadas de API (usando requests) e
processamento de dados.

Este módulo fornece um decorador `tenacity.retry` pré-configurado (`default_retry`)
para lidar com erros transitórios.
"""

import logging

# Dependências de terceiros
# MUDANÇA CRUCIAL: Importar exceções da biblioteca 'requests'
from requests.exceptions import ConnectionError, HTTPError, Timeout
from tenacity import (
    RetryCallState,
    retry,
    stop_after_attempt,
    wait_exponential,
)

# --- Configuração do Logger ---
logger = logging.getLogger(__name__)


def _log_before_sleep(retry_state: RetryCallState):
    """Loga uma mensagem ANTES de o Tenacity entrar em espera entre tentativas."""
    # Esta função já é excelente, sem necessidade de grandes mudanças.
    # Adicionei apenas "..." para indicar que a mensagem de erro foi truncada.
    if not (outcome := retry_state.outcome) or not outcome.failed:
        return

    if not (exception := outcome.exception()):
        return

    if not (next_action := retry_state.next_action) or not hasattr(
        next_action, "sleep"
    ):
        return

    sleep_duration = next_action.sleep
    truncated_exc = str(exception).replace("\n", " ")[:150]

    logger.warning(
        f"Tentativa {retry_state.attempt_number} falhou com "
        f"{type(exception).__name__}: {truncated_exc}... Tentando novamente em "
        f"{sleep_duration:.2f} segundos..."
    )


def should_retry_exception(retry_state: RetryCallState) -> bool:
    """Determina se uma exceção capturada justifica uma nova tentativa."""
    if not retry_state.outcome or not (exception := retry_state.outcome.exception()):
        return False

    # SUGESTÃO DE REFINAMENTO: Tratar os casos simples primeiro (early return).
    # Erros de rede genéricos são sempre transitórios e devem ser tentados novamente.
    if isinstance(exception, (Timeout, ConnectionError)):
        return True

    # Agora, tratar o caso mais complexo do HTTPError.
    if isinstance(exception, HTTPError):
        status_code = exception.response.status_code

        # CASOS TRANSITÓRIOS (TENTAR NOVAMENTE)
        # Erro 429 (Too Many Requests) ou erros 5xx (Server Error)
        if status_code == 429 or status_code >= 500:  # noqa E501
            return True

    # Se a exceção não foi um dos casos transitórios acima, não tente novamente.
    # Isso cobre implicitamente os outros erros 4xx e quaisquer outras exceções.
    return False


default_retry = retry(
    retry=should_retry_exception,
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(3),
    before_sleep=_log_before_sleep,
    reraise=True,
)
