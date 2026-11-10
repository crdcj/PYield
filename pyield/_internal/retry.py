"""Uma retentativa curta no transporte, sem repetir parsing ou processamento."""

import logging
import random
import time
from collections.abc import Callable
from email.utils import parsedate_to_datetime
from http import HTTPStatus

import requests

logger = logging.getLogger(__name__)
_TIMEOUT_CONEXAO_SEGUNDOS = 5
_ESPERA_MINIMA = 0.5
_ESPERA_MAXIMA = 1.0
_STATUS_HTTP_TRANSITORIOS = {502, 503, 504}


def _segundos_retry_after(valor: str) -> float | None:
    try:
        if valor.strip().isdecimal():
            return float(valor)
        data = parsedate_to_datetime(valor)
        if data.tzinfo is not None:
            return max(0.0, data.timestamp() - time.time())
    except (ValueError, TypeError, OverflowError):
        return None
    return None


def _espera_apos_falha(erro: Exception) -> float | None:
    if isinstance(erro, requests.exceptions.SSLError):
        return None
    espera = random.uniform(_ESPERA_MINIMA, _ESPERA_MAXIMA)
    if isinstance(erro, requests.HTTPError):
        resposta = erro.response
        if resposta is None or resposta.status_code not in _STATUS_HTTP_TRANSITORIOS:
            return None
        retry_after = resposta.headers.get("Retry-After")
        if retry_after is not None:
            segundos = _segundos_retry_after(retry_after)
            if segundos is None or segundos > _ESPERA_MAXIMA:
                return None
            espera = max(espera, segundos)
    return espera


def requisitar(
    enviar: Callable[..., requests.Response],
    url: str,
    *,
    timeout_leitura: float,
    **kwargs,
) -> requests.Response:
    """Aplica retry leve a uma consulta HTTP explicitamente segura de repetir.

    O chamador mantém método e sessão e informa apenas o timeout de leitura.
    O timeout de conexão é fixo em 5 segundos. Apenas envio, download e status
    HTTP participam da retentativa; o corpo é processado pelo chamador.

    Há no máximo duas tentativas. Repete somente falhas transitórias de
    comunicação ou HTTP 502/503/504. Retry-After inválido ou superior a um
    segundo impede a retentativa. A segunda falha é propagada, preservando a
    exceção do transporte.
    """

    def baixar() -> requests.Response:
        resposta = enviar(
            url, timeout=(_TIMEOUT_CONEXAO_SEGUNDOS, timeout_leitura), **kwargs
        )
        resposta.raise_for_status()
        if resposta.status_code == HTTPStatus.NOT_MODIFIED:
            # Sem requisição condicional, 304 é resposta inválida da fonte.
            raise requests.HTTPError(
                f"304 Not Modified sem requisição condicional para url: {url}",
                response=resposta,
            )
        return resposta

    try:
        return baixar()
    except (
        requests.ConnectionError,
        requests.Timeout,
        requests.exceptions.ChunkedEncodingError,
        requests.HTTPError,
        ConnectionError,
        TimeoutError,
    ) as erro:
        espera = _espera_apos_falha(erro)
        if espera is None:
            raise
        logger.warning(
            "Falha transitória no download (%s). Última tentativa em %.2f segundos.",
            type(erro).__name__,
            espera,
        )
        time.sleep(espera)
    return baixar()
