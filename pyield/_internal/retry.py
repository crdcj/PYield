import functools
import logging
import random
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar

from requests import exceptions as rex

registro = logging.getLogger(__name__)

# Constantes para valores de retry
_TAMANHO_MAXIMO_EXCECAO = 150
_HTTP_MUITAS_REQUISICOES = 429
_HTTP_ERRO_SERVIDOR_MINIMO = 500
_MAX_TENTATIVAS = 3
_ESPERA_MINIMA = 1.0
_ESPERA_MAXIMA = 10.0
_MULTIPLICADOR_BACKOFF = 2.0

_EXCECOES_TRANSITORIAS = (
    rex.Timeout,
    rex.ConnectionError,
    rex.SSLError,
    rex.ChunkedEncodingError,
)

P = ParamSpec("P")
R = TypeVar("R")


def _logar_antes_espera(
    tentativa: int, excecao: Exception, tempo_espera: float
) -> None:
    """Loga uma mensagem antes da espera entre tentativas."""
    texto_excecao = str(excecao).replace("\n", " ")
    texto_excecao_truncado = texto_excecao[:_TAMANHO_MAXIMO_EXCECAO]
    if len(texto_excecao) > _TAMANHO_MAXIMO_EXCECAO:
        texto_excecao_truncado += "..."

    registro.warning(
        "Tentativa %s falhou com %s: %s Tentando novamente em %.2f segundos...",
        tentativa,
        type(excecao).__name__,
        texto_excecao_truncado,
        tempo_espera,
    )


def _deve_tentar_novamente_por_excecao(excecao: Exception) -> bool:
    """Determina se uma exceção capturada justifica uma nova tentativa."""
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


def _calcular_tempo_espera(tentativa: int) -> float:
    """Calcula o tempo de espera com backoff exponencial e jitter."""
    limite_superior = min(
        _ESPERA_MAXIMA,
        max(_ESPERA_MINIMA, _MULTIPLICADOR_BACKOFF * (2 ** (tentativa - 1))),
    )
    return random.uniform(_ESPERA_MINIMA, limite_superior)


def retry_padrao(func: Callable[P, R]) -> Callable[P, R]:
    """Aplica retry com backoff exponencial e jitter para falhas transitórias."""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        for tentativa in range(1, _MAX_TENTATIVAS + 1):
            try:
                return func(*args, **kwargs)
            except Exception as excecao:
                if (
                    tentativa == _MAX_TENTATIVAS
                    or not _deve_tentar_novamente_por_excecao(excecao)
                ):
                    raise

                tempo_espera = _calcular_tempo_espera(tentativa)
                _logar_antes_espera(tentativa, excecao, tempo_espera)
                time.sleep(tempo_espera)

        msg = "Fluxo de retry inválido: laço encerrado sem retorno nem exceção."
        raise RuntimeError(msg)

    return wrapper
