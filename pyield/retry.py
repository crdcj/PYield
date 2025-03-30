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

# --- Constantes ---
DEFAULT_TIMEOUT = (2, 10)
RETRYABLE_NETWORK_ERRORS = (URLError,)
RETRYABLE_PARSING_ERRORS = (pd.errors.EmptyDataError, pd.errors.ParserError)


# --- Funções Auxiliares para Tenacity ---


def _log_before_sleep(retry_state: RetryCallState):
    """
    Loga uma mensagem ANTES de o Tenacity entrar em espera (sleep) entre tentativas.

    Simplificada para confiar no contrato do Tenacity e usar asserções
    para verificar o estado esperado.
    """
    # 1. Verificar o resultado (outcome)
    outcome = retry_state.outcome
    assert outcome is not None, "before_sleep chamada sem 'outcome'"
    # Adicionado: Garantir que realmente falhou antes de logar como falha
    assert outcome.failed, "before_sleep chamada para 'outcome' bem-sucedido"

    exception = outcome.exception
    # Adicionado: Garantir que a exceção existe quando falhou
    assert exception is not None, "before_sleep chamada sem 'exception' no outcome"

    # 2. Verificar a próxima ação (next_action)
    next_action = retry_state.next_action
    assert next_action is not None, "before_sleep chamada sem 'next_action'"

    # 3. Acessar a duração do sono (sleep)
    # Confiamos que 'next_action' terá '.sleep' neste contexto, pois é antes de dormir.
    sleep_duration = next_action.sleep

    # Log principal
    logger.warning(
        f"Tentativa {retry_state.attempt_number} falhou com "
        f"{type(exception).__name__}: {exception}. Tentando novamente em "
        f"{sleep_duration:.2f} segundos..."
    )


def should_retry_exception(retry_state: RetryCallState) -> bool:
    """Determina se uma exceção capturada justifica uma nova tentativa."""
    if not retry_state.outcome or not retry_state.outcome.failed:
        return False

    # Usar o atributo .exception
    exception = retry_state.outcome.exception
    assert exception is not None, (
        "should_retry_exception processando um outcome sem exceção?"
    )

    attempt_number = retry_state.attempt_number
    url = getattr(exception, "url", "N/A")

    retry_decision = False
    log_message = ""
    log_level = logging.ERROR

    match exception:
        case HTTPError(code=404):
            log_message = (
                f"Tentativa {attempt_number}: Recurso não encontrado (404) em {url}. "
                f"Não tentará novamente."
            )
            retry_decision = False
            log_level = logging.INFO

        case HTTPError(code=504) if attempt_number == 1:
            log_message = (
                f"Tentativa {attempt_number}: Gateway Timeout (504) da API em {url}. "
                f"Tentará novamente uma vez."
            )
            retry_decision = True
            log_level = logging.WARNING

        case HTTPError(code=504):
            log_message = (
                f"Tentativa {attempt_number}: Gateway Timeout (504) persistiu em {url}."
                f"API parece indisponível. Desistindo."
            )
            retry_decision = False
            log_level = logging.ERROR

        case HTTPError() as http_err:
            log_message = (
                f"Tentativa {attempt_number}: Erro HTTP {http_err.code} encontrado "
                f"para {url}. Pode ser transitório. Tentando novamente."
            )
            retry_decision = True
            log_level = logging.WARNING

        case URLError():
            log_message = (
                f"Tentativa {attempt_number}: Erro de rede recuperável "
                f"({type(exception).__name__}). Tentando novamente."
            )
            retry_decision = True
            log_level = logging.WARNING

        case pd.errors.EmptyDataError() | pd.errors.ParserError():
            log_message = (
                f"Tentativa {attempt_number}: Erro de parsing "
                f"({type(exception).__name__}). Tentando novamente."
            )
            retry_decision = True
            log_level = logging.WARNING

        case _:
            log_message_exc = (
                f"Tentativa {attempt_number}: Erro não recuperável/inesperado "
                f"({type(exception).__name__}). Desistindo."
            )
            retry_decision = False
            log_level = logging.ERROR
            logger.exception(log_message_exc)
            log_message = None

    if log_message:
        logger.log(log_level, log_message)

    return retry_decision


# --- Decorador Tenacity Principal ---
default_retry = tenacity.retry(
    retry=should_retry_exception,
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    stop=(tenacity.stop_after_attempt(3) | tenacity.stop_after_delay(15)),
    before_sleep=_log_before_sleep,
    reraise=True,
)
