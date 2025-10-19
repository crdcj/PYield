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

    # Para qualquer outra exceção não listada, não tentar novamente.
    return False


# --- Decorador Tenacity Principal ---
"""Retry policy padrão.

IMPORTANTE SOBRE A CONFIGURAÇÃO DE *stop*:
Antes usávamos: stop=(stop_after_attempt(3) | stop_after_delay(15))
Com chamadas HTTP usando timeout=30s, um único ReadTimeout já consumia >15s
e o Tenacity entendia que a condição de parada OR havia sido atingida,
encerrando o fluxo SEM realizar as tentativas restantes.

Para garantir que até 3 tentativas sejam feitas independentemente da duração
individual (desde que a aplicação queira esperar), removemos o limite de
tempo agregado e mantivemos apenas stop_after_attempt(3).

Se for necessário um limite total de tempo, ele deve ser configurado para um
valor superior a (timeout_http * número_de_tentativas) ou usar um timeout
menor na chamada requests.get.
"""

default_retry = retry(
    retry=should_retry_exception,
    wait=wait_exponential(multiplier=1, min=1, max=10),
    # Apenas limite por número de tentativas para não abortar prematuramente
    stop=stop_after_attempt(3),
    before_sleep=_log_before_sleep,
    reraise=True,
)
