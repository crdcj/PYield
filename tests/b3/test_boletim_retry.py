"""Retentativas do download da B3 pela API pública, sem acesso à rede."""

import time

import pytest
import requests

import pyield as yd
from tests.b3.test_boletim_negociacao import _criar_zip_boletim

TOTAL_TENTATIVAS = 2
TOTAL_CONSULTAS = 2
ESPERA_MINIMA = 0.5


@pytest.fixture
def transporte(monkeypatch):
    chamadas = []
    esperas = []
    monkeypatch.setattr(time, "sleep", esperas.append)
    return chamadas, esperas


def _resposta(status, conteudo=b""):
    resposta = requests.Response()
    resposta.status_code = status
    resposta._content = conteudo
    return resposta


def _erro(falha):
    if isinstance(falha, int):
        return requests.HTTPError(response=_resposta(falha))
    return falha("Falha no download da B3.")


def test_download_recupera_falha_transitoria(monkeypatch, transporte):
    chamadas, esperas = transporte
    conteudo = _criar_zip_boletim()
    erro = _erro(requests.ReadTimeout)

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        assert kwargs["timeout"] == [(5, 5), (5, 10)][len(chamadas) - 1]
        if len(chamadas) == 1:
            raise erro
        return _resposta(200, conteudo)

    monkeypatch.setattr(requests.Session, "send", enviar)

    assert yd.b3.boletim.baixar_zip("12-01-2026") == conteudo
    assert len(chamadas) == TOTAL_TENTATIVAS
    assert len(esperas) == 1
    assert ESPERA_MINIMA <= esperas[0] <= 1


@pytest.mark.parametrize(
    "falha",
    [
        requests.ConnectTimeout,
        requests.ReadTimeout,
        requests.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
        502,
        503,
        504,
    ],
)
def test_consulta_propaga_ultima_falha_apos_duas_tentativas(
    monkeypatch, transporte, falha
):
    chamadas, esperas = transporte
    erros = [_erro(falha) for _ in range(TOTAL_TENTATIVAS)]

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        raise erros[len(chamadas) - 1]

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(type(erros[-1])) as capturado:
        yd.b3.boletim.buscar("12-01-2026", "DI1")

    assert capturado.value is erros[-1]
    assert len(chamadas) == TOTAL_TENTATIVAS
    assert len(esperas) == 1
    assert ESPERA_MINIMA <= esperas[0] <= 1


@pytest.mark.parametrize(
    "falha",
    [
        requests.exceptions.SSLError,
        requests.exceptions.InvalidURL,
        400,
        401,
        403,
        404,
        429,
        500,
        501,
        505,
    ],
)
def test_consulta_nao_repete_demais_erros(monkeypatch, transporte, falha):
    chamadas, esperas = transporte
    erro = _erro(falha)

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        raise erro

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(type(erro)) as capturado:
        yd.b3.boletim.buscar("12-01-2026", "DI1")

    assert capturado.value is erro
    assert len(chamadas) == 1
    assert esperas == []


def test_consulta_recupera_resposta_http_503(monkeypatch, transporte):
    chamadas, esperas = transporte

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        if len(chamadas) == 1:
            return _resposta(503)
        return _resposta(200, _criar_zip_boletim())

    monkeypatch.setattr(requests.Session, "send", enviar)

    assert yd.b3.boletim.buscar("12-01-2026", "DI1").is_empty()
    assert len(chamadas) == TOTAL_CONSULTAS
    assert len(esperas) == 1
    assert ESPERA_MINIMA <= esperas[0] <= 1


def test_zip_invalido_nao_provoca_retry_e_download_nao_mantem_cache(
    monkeypatch, transporte
):
    chamadas, esperas = transporte

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        return _resposta(200, b"ZIP invalido")

    monkeypatch.setattr(requests.Session, "send", enviar)

    assert yd.b3.boletim.baixar_zip("12-01-2026") == b""
    assert yd.b3.boletim.baixar_zip("12-01-2026") == b""
    assert len(chamadas) == TOTAL_CONSULTAS
    assert esperas == []
