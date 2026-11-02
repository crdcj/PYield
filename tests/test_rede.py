"""Política de downloads exercitada pelas consultas públicas, sem acesso à rede."""

import datetime as dt
import importlib
import io
import json
import time
import zipfile
from decimal import Decimal
from http import HTTPStatus
from pathlib import Path

import pytest
import requests

import pyield as yd

data_cache = importlib.import_module("pyield._internal.data_cache")
retry = importlib.import_module("pyield._internal.retry")
NUMERO_PAGINAS = 2
NUMERO_CONSULTAS = 2
TAXA_SELIC_META = Decimal("0.1050000000")
ESPERA_RETRY = 0.75


@pytest.fixture
def sem_espera(monkeypatch):
    def falhar(_segundos):
        pytest.fail("A consulta não deve aguardar para repetir um download.")

    monkeypatch.setattr(time, "sleep", falhar)


@pytest.fixture
def esperas(monkeypatch):
    valores = []
    monkeypatch.setattr(time, "sleep", valores.append)
    monkeypatch.setattr(retry.random, "uniform", lambda a, b: ESPERA_RETRY)
    return valores


@pytest.fixture
def horario_pregao(monkeypatch):
    monkeypatch.setattr(
        yd.relogio,
        "agora",
        lambda: dt.datetime(2026, 10, 30, 13, tzinfo=yd.relogio.BR_TZ),
    )


@pytest.mark.parametrize(
    "consultar",
    [
        pytest.param(lambda: yd.selic.over_serie(ultimos=1), id="selic-over"),
        pytest.param(lambda: yd.selic.meta_serie(ultimos=1), id="selic-meta"),
        pytest.param(lambda: yd.ptax_serie(ultimos=1), id="ptax"),
        pytest.param(lambda: yd.igpm.taxa_serie(ultimos=1), id="igpm-taxa"),
        pytest.param(lambda: yd.igpm.indice("01-01-2025"), id="igpm-indice"),
        pytest.param(lambda: yd.ipca.indice("01-01-2025"), id="ipca-indice"),
        pytest.param(lambda: yd.ipca.calendario(2026), id="calendario-ipca"),
        pytest.param(lambda: yd.igpm.calendario(2026), id="calendario-igpm"),
        pytest.param(yd.copom.calendario, id="copom"),
        pytest.param(yd.compromissadas, id="olinda"),
        pytest.param(lambda: yd.tpf.leiloes(data="19-08-2025"), id="leiloes"),
        pytest.param(lambda: yd.tpf.estoque("04-02-2026"), id="estoque"),
        pytest.param(yd.tpf.benchmarks, id="benchmarks"),
        pytest.param(yd.tpf.dealers, id="dealers"),
        pytest.param(
            lambda: yd.tpf.secundario.mensal("01-01-2025"), id="secundario-mensal"
        ),
        pytest.param(lambda: yd.rmd(aba="1.3"), id="rmd"),
        pytest.param(lambda: yd.vna.valor("LFT", "31-05-2024"), id="vna-lft"),
        pytest.param(lambda: yd.vna.ultimo("NTN-B"), id="vna-planilha"),
        pytest.param(lambda: yd.b3.boletim.buscar("12-01-2026"), id="boletim-b3"),
        pytest.param(lambda: yd.futuro.intradia("DI1"), id="futuro-intradia"),
        pytest.param(lambda: yd.cpm.contratos("29-01-2025"), id="cpm-boletim"),
        pytest.param(yd.tpf.taxas_historicas, id="parquet-tpf"),
        pytest.param(lambda: yd.futuro.historico("12-01-2026", "DI1"), id="parquet"),
    ],
)
@pytest.mark.parametrize(
    "tipo_erro",
    [
        requests.ConnectTimeout,
        requests.ReadTimeout,
        requests.ConnectionError,
        requests.exceptions.SSLError,
        requests.exceptions.ChunkedEncodingError,
        requests.HTTPError,
    ],
)
@pytest.mark.usefixtures("horario_pregao")
def test_consulta_repete_apenas_falha_transitoria(
    monkeypatch, esperas, consultar, tipo_erro
):
    erro = tipo_erro("Fonte indisponível.")
    resposta = requests.Response()
    resposta.status_code = 503
    erro.response = resposta
    chamadas = []

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        raise erro

    data_cache._obter_dataset_com_ttl.cache_clear()
    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(tipo_erro) as capturado:
        consultar()

    assert capturado.value is erro
    transitoria = tipo_erro is not requests.exceptions.SSLError
    assert len(chamadas) == (NUMERO_CONSULTAS if transitoria else 1)
    assert esperas == ([ESPERA_RETRY] if transitoria else [])
    assert len(set(chamadas)) == 1


@pytest.mark.parametrize("falha", [requests.ReadTimeout, 503])
@pytest.mark.parametrize("recuperar", [True, False])
def test_cpm_repete_apenas_post_preservando_payload(
    monkeypatch, esperas, falha, recuperar
):
    dados = Path(__file__).parent
    zip_externo = io.BytesIO()
    with zipfile.ZipFile(zip_externo, "w") as arquivo:
        arquivo.writestr(
            "SPRD250203.zip", (dados / "b3/data/SPRD250203.zip").read_bytes()
        )
    fontes = {
        "https://www.b3.com.br/pesquisapregao/download?filelist=SPRD250203.zip": zip_externo.getvalue(),
        "https://www.bcb.gov.br/api/servico/sitebcb/copom/atas?quantidade=500": (
            dados / "copom/data/atas.json"
        ).read_bytes(),
        "https://www.bcb.gov.br/api/exportarics/sitebcb/agendaics"
        "?lista=Reuni%C3%B5es%20do%20Copom": (
            dados / "copom/data/calendario.ics"
        ).read_bytes(),
    }
    consultas = []
    posts = []
    erro = requests.ReadTimeout("Exportação da B3 indisponível.")
    indisponivel = requests.Response()
    indisponivel.status_code = 503

    def enviar(self, requisicao, **kwargs):
        if requisicao.method == "GET":
            consultas.append(requisicao.url)
            resposta = requests.Response()
            resposta.status_code = 200
            resposta._content = fontes[requisicao.url]
            return resposta

        assert requisicao.method == "POST"
        assert kwargs["timeout"] == (5, 30)
        posts.append(requisicao)
        if len(posts) == 1 or not recuperar:
            if falha is requests.ReadTimeout:
                raise erro
            return indisponivel
        resposta = requests.Response()
        resposta.status_code = 200
        resposta._content = (
            "Derivativos consolidados\n03/02/2025\n"
            "Instrumento financeiro;Preço de referência\n"
            "CPMH25C100000;12,50\n"
        ).encode()
        return resposta

    monkeypatch.setattr(requests.Session, "send", enviar)

    if recuperar:
        resultado = yd.cpm.contratos("03-02-2025")
        assert resultado.select(
            "codigo_negociacao", "preco_ajuste"
        ).drop_nulls().rows() == [("CPMH25C100000", 12.5)]
    else:
        tipo_erro = (
            requests.ReadTimeout
            if falha is requests.ReadTimeout
            else requests.HTTPError
        )
        with pytest.raises(tipo_erro) as capturado:
            yd.cpm.contratos("03-02-2025")
        if falha is requests.ReadTimeout:
            assert capturado.value is erro
        else:
            assert capturado.value.response is indisponivel

    assert consultas == list(fontes)
    assert len(posts) == NUMERO_CONSULTAS
    assert esperas == [ESPERA_RETRY]
    assert (
        posts[0].url
        == posts[1].url
        == ("https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR")
    )
    assert posts[0].body == posts[1].body
    assert json.loads(posts[0].body) == {
        "Name": "ConsolidatedTradesDerivatives",
        "Date": "2025-02-03",
        "FinalDate": "2025-02-03",
        "ClientId": "",
        "Filters": {},
    }
    assert posts[0].headers == posts[1].headers
    assert posts[0].headers["Content-Type"] == "application/json"


@pytest.mark.usefixtures("sem_espera")
@pytest.mark.parametrize("status", [400, 401, 403, 429, 500, 502, 503, 504])
def test_sgs_propaga_resposta_http_sem_repetir(monkeypatch, status):
    resposta = requests.Response()
    resposta.status_code = status
    resposta.headers["Retry-After"] = "120"
    chamadas = []

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        return resposta

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(requests.HTTPError) as capturado:
        yd.selic.meta_serie(ultimos=1)

    assert capturado.value.response is resposta
    assert len(chamadas) == 1


def test_sgs_http_404_preserva_ausencia_de_dados(monkeypatch):
    resposta = requests.Response()
    resposta.status_code = 404
    monkeypatch.setattr(requests.Session, "send", lambda *a, **kw: resposta)

    assert yd.selic.meta_serie(ultimos=1).is_empty()


@pytest.mark.parametrize("falha_no_bloco", [1, 2])
def test_selic_meta_interrompe_blocos_apos_retry(monkeypatch, esperas, falha_no_bloco):
    chamadas = []
    erro = requests.ReadTimeout("SGS indisponível.")

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        if len(chamadas) >= falha_no_bloco:
            raise erro
        resposta = requests.Response()
        resposta.status_code = 200
        resposta._content = b"[]"
        return resposta

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(requests.ReadTimeout) as capturado:
        yd.selic.meta_serie("05-03-1999", "30-10-2026")

    assert capturado.value is erro
    assert len(chamadas) == falha_no_bloco + 1
    assert chamadas[-1] == chamadas[-2]
    assert esperas == [ESPERA_RETRY]


def test_ipca_repete_apenas_pagina_que_falhou(monkeypatch, esperas):
    paginas = []
    erro = requests.ReadTimeout("IBGE indisponível.")

    def enviar(self, requisicao, **kwargs):
        paginas.append(requisicao.url)
        if len(paginas) > 1:
            raise erro
        resposta = requests.Response()
        resposta.status_code = 200
        resposta._content = b'{"items": [], "nextPage": 2}'
        return resposta

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(requests.ReadTimeout) as capturado:
        yd.ipca.calendario(2026)

    assert capturado.value is erro
    assert len(paginas) == NUMERO_PAGINAS + 1
    assert paginas[0].endswith("page=1")
    assert paginas[1].endswith("page=2")
    assert paginas[2] == paginas[1]
    assert esperas == [ESPERA_RETRY]


def test_consulta_recupera_apos_falha_transitoria(monkeypatch, esperas):
    chamadas = []

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        resposta = requests.Response()
        if len(chamadas) == 1:
            resposta.status_code = 503
        else:
            resposta.status_code = 200
            resposta._content = b'[{"data":"31/05/2024","valor":"10.50"}]'
        return resposta

    monkeypatch.setattr(requests.Session, "send", enviar)

    assert yd.selic.meta("31-05-2024") == TAXA_SELIC_META
    assert len(chamadas) == NUMERO_CONSULTAS
    assert esperas == [ESPERA_RETRY]


def test_ipca_projetado_preserva_connection_error_apos_retry(monkeypatch, esperas):
    chamadas = []

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        raise requests.ReadTimeout("ANBIMA indisponível.")

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(ConnectionError, match="ANBIMA"):
        yd.ipca.taxa_projetada()

    assert len(chamadas) == NUMERO_CONSULTAS
    assert esperas == [ESPERA_RETRY]


@pytest.mark.parametrize("status", [503, 403])
def test_di_over_repete_apenas_erro_http_transitorio(monkeypatch, esperas, status):
    resposta = requests.Response()
    resposta.status_code = status
    chamadas = []

    def enviar(self, requisicao, **kwargs):
        chamadas.append(requisicao.url)
        return resposta

    monkeypatch.setattr(requests.Session, "send", enviar)

    with pytest.raises(requests.HTTPError) as capturado:
        yd.di_over("28-02-2025")

    assert capturado.value.response is resposta
    transitorio = status == HTTPStatus.SERVICE_UNAVAILABLE
    assert len(chamadas) == (NUMERO_CONSULTAS if transitorio else 1)
    assert esperas == ([ESPERA_RETRY] if transitorio else [])


@pytest.mark.usefixtures("sem_espera")
def test_di_over_http_404_preserva_ausencia_de_dados(monkeypatch):
    resposta = requests.Response()
    resposta.status_code = 404
    monkeypatch.setattr(requests.Session, "send", lambda *a, **kw: resposta)

    assert yd.di_over("28-02-2025").is_nan()
