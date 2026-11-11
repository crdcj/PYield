"""Limites e Retry-After exercitados pelas consultas públicas."""

import importlib
import time

import pytest
import requests

import pyield as yd

retry = importlib.import_module("pyield._internal.retry")
TOTAL_TENTATIVAS = 2
ESPERA = 0.75
ESPERA_MAXIMA = 1.0


@pytest.fixture
def transporte(monkeypatch):
    chamadas = []
    esperas = []
    monkeypatch.setattr(time, "sleep", esperas.append)

    def sortear(minimo, maximo):
        assert (minimo, maximo) == (0.5, 1.0)
        return ESPERA

    monkeypatch.setattr(retry.random, "uniform", sortear)
    return chamadas, esperas


@pytest.mark.parametrize(
    ("retry_after", "repetir"),
    [
        (None, True),
        ("0", True),
        ("1", True),
        ("2", False),
        ("120", False),
        ("inválido", False),
        ("-1", False),
        ("1.5", False),
        ("Tue, 01 Jan 2030 00:00:00 GMT", True),
        ("Tue, 01 Jan 2030 00:00:01 GMT", True),
        ("Tue, 01 Jan 2030 00:00:02 GMT", False),
        ("Tue, 01 Jan 2030 00:00:01", False),
    ],
)
def test_retry_after_respeita_limite_curto(
    monkeypatch, transporte, retry_after, repetir
):
    chamadas, esperas = transporte
    monkeypatch.setattr(time, "time", lambda: 1893456000.0)
    indisponivel = requests.Response()
    indisponivel.status_code = 503
    if retry_after is not None:
        indisponivel.headers["Retry-After"] = retry_after

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        if len(chamadas) == 1:
            return indisponivel
        resposta = requests.Response()
        resposta.status_code = 200
        resposta._content = b"[]"
        return resposta

    monkeypatch.setattr(requests.Session, "send", enviar)

    if repetir:
        assert yd.selic.meta_serie(ultimos=1).is_empty()
    else:
        with pytest.raises(requests.HTTPError) as capturado:
            yd.selic.meta_serie(ultimos=1)
        assert capturado.value.response is indisponivel

    assert len(chamadas) == (TOTAL_TENTATIVAS if repetir else 1)
    assert len(esperas) == int(repetir)
    if repetir:
        assert ESPERA <= esperas[0] <= ESPERA_MAXIMA
        if retry_after in {"1", "Tue, 01 Jan 2030 00:00:01 GMT"}:
            assert esperas == [ESPERA_MAXIMA]


def test_segunda_falha_e_propagada_sem_terceira_tentativa(monkeypatch, transporte):
    chamadas, esperas = transporte
    erros = [requests.ConnectTimeout("Primeira falha"), requests.ReadTimeout("Última")]

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        raise erros[len(chamadas) - 1]

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(requests.ReadTimeout) as capturado:
        yd.selic.meta_serie(ultimos=1)

    assert capturado.value is erros[-1]
    assert len(chamadas) == TOTAL_TENTATIVAS
    assert esperas == [ESPERA]


def test_json_invalido_nao_provoca_novo_download(monkeypatch, transporte):
    chamadas, esperas = transporte

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        resposta = requests.Response()
        resposta.status_code = 200
        resposta._content = b"JSON invalido"
        return resposta

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(requests.exceptions.JSONDecodeError):
        yd.selic.meta_serie(ultimos=1)

    assert len(chamadas) == 1
    assert esperas == []


def test_304_sem_requisicao_condicional_e_erro_http(monkeypatch, transporte):
    chamadas, esperas = transporte
    nao_modificado = requests.Response()
    nao_modificado.status_code = 304
    nao_modificado._content = b""

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        return nao_modificado

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(requests.HTTPError, match="304 Not Modified") as capturado:
        yd.selic.meta_serie(ultimos=1)

    assert capturado.value.response is nao_modificado
    assert len(chamadas) == 1
    assert esperas == []


@pytest.mark.parametrize(
    ("consultar", "timeout_esperado"),
    [
        (lambda: yd.tpf.estoque("04-02-2026"), (5, 5)),
        (lambda: yd.selic.meta_serie(ultimos=1), (5, 15)),
        (lambda: yd.vna.ultimo("NTN-B"), (5, 5)),
        (lambda: yd.cpm.contratos("29-01-2025"), (5, 5)),
        (lambda: yd.igpm.calendario(2026), (5, 5)),
    ],
)
def test_consultas_separam_conexao_e_leitura(monkeypatch, consultar, timeout_esperado):
    timeouts = []
    erro = requests.exceptions.SSLError("Interrompe antes do download.")

    def enviar(self, requisicao, **kwargs):
        timeouts.append(kwargs["timeout"])
        raise erro

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(requests.exceptions.SSLError) as capturado:
        consultar()

    assert capturado.value is erro
    assert timeouts == [timeout_esperado]
