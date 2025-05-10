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


def should_retry_exception(retry_state: RetryCallState) -> bool:
    """Determina se uma exceção capturada justifica uma nova tentativa"""
    if not retry_state.outcome or not retry_state.outcome.failed:
        return False

    exception = retry_state.outcome.exception()

    if exception is None:
        # Isso não deveria acontecer se outcome.failed é True.
        logger.error(
            "should_retry_exception: outcome.failed é True,"
            "mas exception() retornou None."
        )
        return False

    attempt_number = retry_state.attempt_number
    # getattr é mais seguro caso a exceção não tenha 'url'
    url = getattr(exception, "url", "N/A")

    retry_decision = False
    # Inicializar como None para evitar log default se nenhum case corresponder
    log_message = None
    log_level = logging.ERROR  # Default para não retentativa

    # Usando match/case como originalmente preferido
    match exception:
        case HTTPError(code=404):
            log_message = (
                f"Tentativa {attempt_number}: Recurso não encontrado (404) em {url}. "
                f"Não tentará novamente."
            )
            retry_decision = False
            log_level = logging.INFO

        case HTTPError(code=429):  # ADICIONADO: Tratamento para Too Many Requests
            log_message = (
                f"Tentativa {attempt_number}: Too Many Requests (429) da API em {url}. "
                f"API pediu para aguardar. Tentando novamente."
            )
            retry_decision = True
            log_level = logging.WARNING

        # Gateway Timeout, primeira tentativa
        case HTTPError(code=504) if attempt_number == 1:
            log_message = (
                f"Tentativa {attempt_number}: Gateway Timeout (504) da API em {url}. "
                f"Tentará novamente uma vez."
            )
            retry_decision = True
            log_level = logging.WARNING

        case HTTPError(code=504):  # Gateway Timeout, tentativas subsequentes
            log_message = (
                f"Tentativa {attempt_number}: Gateway Timeout (504) persistiu em {url}."
                f"API parece indisponível. Desistindo."
            )
            retry_decision = False
            log_level = logging.ERROR

        case HTTPError() as http_err:  # Outros erros HTTP
            # Esta regra genérica para HTTPError retentará outros códigos HTTP.
            # Por exemplo, 500, 502, 503 (geralmente bom para retry)
            # mas também 400, 401, 403 (geralmente não bom para retry).
            # Se precisar de mais granularidade, adicione cases específicos acima deste.
            log_message = (
                f"Tentativa {attempt_number}: Erro HTTP {http_err.code} "
                f"({http_err.reason}) encontrado para {url}. "
                "Pode ser transitório. Tentando novamente."
            )
            retry_decision = True
            log_level = logging.WARNING

        # Erros de rede mais genéricos (e.g., DNS falhou, conexão recusada)
        case URLError():
            log_message = (
                f"Tentativa {attempt_number}: Erro de rede recuperável "
                f"({type(exception).__name__}: {str(exception)[:100]}) para {url}. "
                f"Tentando novamente."
            )
            retry_decision = True
            log_level = logging.WARNING

        # Usando o operador | para combinar tipos de exceção no case
        case pd.errors.EmptyDataError() | pd.errors.ParserError():
            log_message = (
                f"Tentativa {attempt_number}: Erro de parsing de dados "
                f"({type(exception).__name__}). Tentando novamente."
            )
            retry_decision = True
            log_level = logging.WARNING

        case _:  # Exceção não mapeada/inesperada
            # Mensagem para o log de exceção
            log_message_exc = (
                f"Tentativa {attempt_number}: Erro não recuperável/inesperado "
                f"({type(exception).__name__}: {str(exception)[:100]}) "
                f"ao processar {url}. Desistindo."
            )
            # Sempre incluir traceback para erros inesperados
            logger.exception(log_message_exc)
            log_message = None  # Evita log duplicado pela seção logger.log abaixo
            retry_decision = False
            log_level = logging.ERROR  # Já é o default, mas explícito aqui

    if log_message:  # Loga a mensagem formatada se uma foi definida
        logger.log(log_level, log_message)

    return retry_decision


# --- Decorador Tenacity Principal ---
default_retry = tenacity.retry(
    retry=should_retry_exception,
    # Backoff exponencial é bom para 429
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    stop=(tenacity.stop_after_attempt(3) | tenacity.stop_after_delay(15)),
    before_sleep=_log_before_sleep,
    reraise=True,  # Importante para propagar o erro final se as tentativas falharem
)
