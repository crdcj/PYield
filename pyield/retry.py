import logging

from requests.exceptions import ConnectionError, HTTPError, Timeout
from tenacity import (
    RetryCallState,
    retry,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class DataNotAvailableError(Exception):
    """Levantada quando o dado baixado for considerado inválido (dado vazio/pequeno)."""

    pass


def _log_before_sleep(retry_state: RetryCallState):
    """Loga uma mensagem ANTES de o Tenacity entrar em espera entre tentativas."""
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

    # Erros de rede genéricos são sempre transitórios
    if isinstance(exception, (Timeout, ConnectionError, DataNotAvailableError)):
        return True

    # HTTPError: apenas 429 e 5xx são transitórios
    if isinstance(exception, HTTPError):
        status_code = exception.response.status_code
        if status_code == 429 or status_code >= 500:  # noqa
            return True

    return False


# Retry policy padrão otimizado para timeouts específicos
default_retry = retry(
    retry=should_retry_exception,
    wait=wait_exponential(multiplier=2, min=1, max=10),
    stop=stop_after_attempt(3),
    before_sleep=_log_before_sleep,
    reraise=True,
)
