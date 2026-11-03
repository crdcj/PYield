import datetime as dt
import importlib
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest
import requests

import pyield as yd

sgs = importlib.import_module("pyield.bc.sgs")
TAXA_DI_OVER = Decimal("0.1315")


@pytest.mark.parametrize("data", ["28-02-2025", "28/02/2025", "2025-02-28"])
def test_di_over_consulta_sgs_e_preserva_taxa_anualizada(monkeypatch, data):
    urls = []

    def chamar_api(url):
        urls.append(url)
        return [
            {"data": "27/02/2025", "valor": "13.15"},
            {"data": "28/02/2025", "valor": "13.15"},
        ]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    assert yd.di_over(data) == TAXA_DI_OVER
    assert yd.di_over is sgs.di_over
    assert len(urls) == 1
    url = urlparse(urls[0])
    assert url.scheme == "https"
    assert url.path == "/dados/serie/bcdata.sgs.4389/dados"
    assert parse_qs(url.query) == {
        "formato": ["json"],
        "dataInicial": ["27/02/2025"],
        "dataFinal": ["28/02/2025"],
    }


def test_di_over_sem_dados(monkeypatch):
    monkeypatch.setattr(sgs, "_chamar_api", lambda _: [])

    assert yd.di_over("28/02/2025").is_nan()


def test_di_over_serie_namespace_publico():
    assert "di_over_serie" in yd.__all__
    assert yd.di_over_serie is sgs.di_over_serie
    assert not hasattr(yd.bc, "di_over_serie")
    assert not hasattr(yd.b3, "di_over_serie")


@pytest.mark.parametrize("inicio", ["27-02-2025", "27/02/2025", "2025-02-27"])
@pytest.mark.parametrize(
    ("anualizada", "publicado", "esperado"),
    [
        (True, "13.15", Decimal("0.1315")),
        (False, "0.049037", Decimal("0.00049037")),
    ],
)
def test_di_over_serie_consulta_intervalo_em_uma_requisicao(
    monkeypatch, inicio, anualizada, publicado, esperado
):
    urls = []

    def chamar_api(url):
        urls.append(url)
        return [
            {"data": "28/02/2025", "valor": publicado},
            {"data": "27/02/2025", "valor": publicado},
            {"data": "28/02/2025", "valor": publicado},
        ]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    resultado = yd.di_over_serie(inicio, "28/02/2025", anualizada=anualizada)

    escala = 4 if anualizada else 8
    assert resultado.schema == {"data": pl.Date, "taxa": pl.Decimal(18, escala)}
    assert resultado.to_dicts() == [
        {"data": dt.date(2025, 2, 27), "taxa": esperado},
        {"data": dt.date(2025, 2, 28), "taxa": esperado},
    ]
    assert len(urls) == 1
    url = urlparse(urls[0])
    codigo = 4389 if anualizada else 12
    assert url.scheme == "https"
    assert url.path == f"/dados/serie/bcdata.sgs.{codigo}/dados"
    assert parse_qs(url.query) == {
        "formato": ["json"],
        "dataInicial": ["27/02/2025"],
        "dataFinal": ["28/02/2025"],
    }


@pytest.mark.parametrize(
    ("anualizada", "codigo", "publicado", "esperado"),
    [
        (True, 4389, "13.15", Decimal("0.1315")),
        (False, 12, "0.049037", Decimal("0.00049037")),
    ],
)
def test_di_over_serie_ultimos_tem_prioridade(
    monkeypatch, anualizada, codigo, publicado, esperado
):
    urls = []

    def chamar_api(url):
        urls.append(url)
        return [{"data": "28/02/2025", "valor": publicado}]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    resultado = yd.di_over_serie(
        "invalida", "invalida", ultimos=1, anualizada=anualizada
    )

    assert resultado.to_dicts() == [{"data": dt.date(2025, 2, 28), "taxa": esperado}]
    assert urls == [f"{sgs.URL_BASE}{codigo}/dados/ultimos/1?formato=json"]


@pytest.mark.parametrize("anualizada", [True, False])
@pytest.mark.parametrize("nao_encontrado", [True, False])
def test_di_over_serie_sem_dados_preserva_esquema(
    monkeypatch, anualizada, nao_encontrado
):
    def chamar_api(_url):
        if nao_encontrado:
            resposta = requests.Response()
            resposta.status_code = 404
            raise requests.HTTPError(response=resposta)
        return []

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    resultado = yd.di_over_serie("27/02/2025", "28/02/2025", anualizada=anualizada)

    assert resultado.is_empty()
    escala = 4 if anualizada else 8
    assert resultado.schema == {"data": pl.Date, "taxa": pl.Decimal(18, escala)}


def test_di_over_serie_um_dia_sem_observacao_omite_dia_anterior(monkeypatch):
    monkeypatch.setattr(
        sgs,
        "_chamar_api",
        lambda _: [{"data": "31/12/2024", "valor": "12.15"}],
    )

    resultado = yd.di_over_serie("01/01/2025", "01/01/2025")

    assert resultado.is_empty()
    assert resultado.schema == {"data": pl.Date, "taxa": pl.Decimal(18, 4)}


def test_di_over_serie_fim_padrao_usa_hoje(monkeypatch):
    monkeypatch.setattr(sgs.relogio, "hoje", lambda: dt.date(2025, 2, 28))

    def chamar_api(url):
        assert parse_qs(urlparse(url).query)["dataFinal"] == ["28/02/2025"]
        return [{"data": "28/02/2025", "valor": "13.15"}]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    assert yd.di_over_serie("27/02/2025").to_dicts() == [
        {"data": dt.date(2025, 2, 28), "taxa": TAXA_DI_OVER}
    ]


def test_di_over_serie_intervalo_invertido_nao_consulta_api(monkeypatch):
    def falhar_se_chamada(_url):
        pytest.fail("Intervalo invertido não deve consultar a API.")

    monkeypatch.setattr(sgs, "_chamar_api", falhar_se_chamada)

    resultado = yd.di_over_serie("28/02/2025", "27/02/2025")

    assert resultado.is_empty()
    assert resultado.schema == {"data": pl.Date, "taxa": pl.Decimal(18, 4)}


def test_di_over_serie_exige_inicio_ou_ultimos():
    with pytest.raises(ValueError, match="inicio.*ultimos"):
        yd.di_over_serie()


def test_di_over_serie_anualizada_exige_argumento_por_nome():
    with pytest.raises(TypeError):
        yd.di_over_serie("27/02/2025", "28/02/2025", False)
