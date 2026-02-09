import logging

import tenacity as tnc
from requests import exceptions as rex

registro = logging.getLogger(__name__)

# Constantes para valores de retry
_TAMANHO_MAXIMO_EXCECAO = 150
_HTTP_MUITAS_REQUISICOES = 429
_HTTP_ERRO_SERVIDOR_MINIMO = 500


class DadoIndisponivelError(Exception):
    """Levantada quando o dado baixado for considerado inválido (dado vazio/pequeno)."""


_EXCECOES_TRANSITORIAS = (
    rex.Timeout,
    rex.ConnectionError,
    rex.SSLError,
    rex.ChunkedEncodingError,
    DadoIndisponivelError,
)


def _logar_antes_espera(retry_state: tnc.RetryCallState) -> None:
    """Loga uma mensagem ANTES de o Tenacity entrar em espera entre tentativas."""
    if not (desfecho := retry_state.outcome) or not desfecho.failed:
        return

    if not (excecao := desfecho.exception()):
        return

    if not (proxima_acao := retry_state.next_action) or not hasattr(
        proxima_acao, "sleep"
    ):
        return

    tempo_espera = proxima_acao.sleep
    texto_excecao = str(excecao).replace("\n", " ")
    texto_excecao_truncado = texto_excecao[:_TAMANHO_MAXIMO_EXCECAO]
    if len(texto_excecao) > _TAMANHO_MAXIMO_EXCECAO:
        texto_excecao_truncado += "..."

    registro.warning(
        f"Tentativa {retry_state.attempt_number} falhou com "
        f"{type(excecao).__name__}: {texto_excecao_truncado} Tentando novamente em "
        f"{tempo_espera:.2f} segundos..."
    )


def _deve_tentar_novamente_por_excecao(retry_state: tnc.RetryCallState) -> bool:
    """Determina se uma exceção capturada justifica uma nova tentativa."""
    if not retry_state.outcome or not (excecao := retry_state.outcome.exception()):
        return False

    # Erros de rede genéricos são sempre transitórios
    if isinstance(excecao, _EXCECOES_TRANSITORIAS):
        return True

    # HTTPError: apenas 429 e 5xx são transitórios
    if isinstance(excecao, rex.HTTPError) and excecao.response is not None:
        codigo_status = excecao.response.status_code
        return (
            codigo_status == _HTTP_MUITAS_REQUISICOES
            or codigo_status >= _HTTP_ERRO_SERVIDOR_MINIMO
        )

    return False


# Retry policy padrão com backoff exponencial + jitter para reduzir rajadas
retry_padrao = tnc.retry(
    retry=_deve_tentar_novamente_por_excecao,
    wait=tnc.wait_random_exponential(multiplier=2, min=1, max=10),
    stop=tnc.stop_after_attempt(3),
    before_sleep=_logar_antes_espera,
    reraise=True,
)
