import datetime as dt
import importlib
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest

import pyield as yd

sgs = importlib.import_module("pyield.bc.sgs")


@pytest.mark.parametrize("consultar", [yd.selic.over, yd.di_over])
@pytest.mark.parametrize(
    ("anualizada", "publicado", "esperado"),
    [
        (True, "13.15", Decimal("0.1315000000")),
        (False, "0.04903712", Decimal("0.0004903712")),
    ],
)
def test_over_seleciona_serie_e_preserva_decimal_da_fonte(
    monkeypatch, consultar, anualizada, publicado, esperado
):
    urls = []

    def chamar_api(url):
        urls.append(url)
        return [{"data": "28/02/2025", "valor": publicado}]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    assert consultar("28/02/2025", anualizada=anualizada) == esperado
    if consultar is yd.selic.over:
        codigo = 1178 if anualizada else 11
    else:
        codigo = 4389 if anualizada else 12
    assert len(urls) == 1
    assert urlparse(urls[0]).path == f"/dados/serie/bcdata.sgs.{codigo}/dados"


@pytest.mark.parametrize(
    "consulta",
    [
        {"inicio": "28/02/2025", "fim": "28/02/2025"},
        {"ultimos": 1},
    ],
)
def test_selic_over_serie_diaria_por_periodo_ou_ultimos(monkeypatch, consulta):
    urls = []

    def chamar_api(url):
        urls.append(url)
        return [{"data": "28/02/2025", "valor": "0.04903712"}]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    resultado = yd.selic.over_serie(**consulta, anualizada=False)

    assert resultado.schema == {"data": pl.Date, "taxa": pl.Decimal(18, 10)}
    assert resultado.to_dicts() == [
        {"data": dt.date(2025, 2, 28), "taxa": Decimal("0.0004903712")}
    ]
    assert len(urls) == 1
    url = urlparse(urls[0])
    if "ultimos" in consulta:
        assert url.path == "/dados/serie/bcdata.sgs.11/dados/ultimos/1"
    else:
        assert url.path == "/dados/serie/bcdata.sgs.11/dados"
        assert parse_qs(url.query)["dataFinal"] == ["28/02/2025"]


@pytest.mark.parametrize("consultar", [yd.selic.over, yd.di_over])
@pytest.mark.parametrize("anualizada", [True, False])
def test_over_sem_observacao_nao_reutiliza_dia_anterior(
    monkeypatch, consultar, anualizada
):
    monkeypatch.setattr(
        sgs,
        "_chamar_api",
        lambda _: [{"data": "31/12/2024", "valor": "0.045513"}],
    )

    assert consultar("01/01/2025", anualizada=anualizada).is_nan()


def test_selic_over_serie_diaria_vazia_preserva_esquema(monkeypatch):
    monkeypatch.setattr(sgs, "_chamar_api", lambda _: [])

    resultado = yd.selic.over_serie(ultimos=1, anualizada=False)

    assert resultado.is_empty()
    assert resultado.schema == {"data": pl.Date, "taxa": pl.Decimal(18, 10)}


@pytest.mark.parametrize("consultar", [yd.selic.over, yd.di_over])
def test_over_diaria_entrada_vazia_nao_consulta_api(monkeypatch, consultar):
    def falhar_se_chamada(_url):
        pytest.fail("Entrada vazia não deve consultar a API.")

    monkeypatch.setattr(sgs, "_chamar_api", falhar_se_chamada)

    assert consultar(None, anualizada=False).is_nan()


@pytest.mark.parametrize("consultar", [yd.selic.over, yd.di_over])
def test_over_anualizada_exige_argumento_por_nome(consultar):
    with pytest.raises(TypeError):
        consultar("28/02/2025", False)
