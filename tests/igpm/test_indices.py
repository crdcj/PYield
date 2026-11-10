"""Consulta pública dos números-índice com JSON real."""

import datetime as dt
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

import pyield as yd
from pyield.igpm import _indices as igpm  # noqa: PLC2701

_DADOS = Path(__file__).parent / "data" / "igpm_ipeadata_202609.json"
_INICIO_COBERTURA = 199408
_ESQUEMA = {"periodo": pl.Int64, "indice": pl.Decimal(precision=18, scale=3)}


@pytest.fixture(autouse=True)
def historico_local(monkeypatch):
    monkeypatch.setattr(igpm, "_buscar_historico", _DADOS.read_bytes)


def test_namespace_publico():
    assert yd.igpm.indice is igpm.indice
    assert yd.igpm.indice_serie is igpm.indice_serie


@pytest.mark.parametrize("data", ["01-06-2000", "30/06/2000", "2000-06-15"])
def test_indice_por_competencia(data):
    resultado = yd.igpm.indice(data)

    assert isinstance(resultado, Decimal)
    assert resultado.as_tuple() == Decimal("183.745").as_tuple()


@pytest.mark.parametrize("data", ["01-06-1989", "01-07-1994", "31-07-1994"])
def test_indice_anterior_a_agosto_1994_retorna_nan_sem_buscar(monkeypatch, data):
    def falhar_fetch():
        raise AssertionError("O histórico não deveria ser buscado.")

    monkeypatch.setattr(igpm, "_buscar_historico", falhar_fetch)

    assert yd.igpm.indice(data).is_nan()


def test_serie_intervalo_inclusivo_por_mes():
    resultado = yd.igpm.indice_serie("15-06-2000", "20-07-2000")

    assert resultado.schema == _ESQUEMA
    assert resultado.to_dicts() == [
        {"periodo": 200006, "indice": Decimal("183.745")},
        {"periodo": 200007, "indice": Decimal("186.634")},
    ]


def test_serie_fim_padrao_usa_hoje(monkeypatch):
    monkeypatch.setattr(igpm.relogio, "hoje", lambda: dt.date(2000, 7, 20))

    resultado = yd.igpm.indice_serie("15-06-2000")

    assert resultado["periodo"].to_list() == [200006, 200007]


def test_ultimos_tem_prioridade():
    resultado = yd.igpm.indice_serie("invalida", "invalida", ultimos=1)

    assert resultado.schema == _ESQUEMA
    assert resultado.to_dicts() == [{"periodo": 202609, "indice": Decimal("1225.846")}]


def test_serie_omite_competencias_anteriores_ao_corte():
    resultado = yd.igpm.indice_serie("01-06-1989", "30-09-1994")

    assert resultado.schema == _ESQUEMA
    assert resultado.to_dicts() == [
        {"periodo": 199408, "indice": Decimal("100.000")},
        {"periodo": 199409, "indice": Decimal("101.751")},
    ]
    assert yd.igpm.indice("01-08-1994").as_tuple() == Decimal("100.000").as_tuple()


def test_intervalo_anterior_ao_corte_retorna_vazio_sem_buscar(monkeypatch):
    def falhar_fetch():
        raise AssertionError("O histórico não deveria ser buscado.")

    monkeypatch.setattr(igpm, "_buscar_historico", falhar_fetch)

    resultado = yd.igpm.indice_serie("01-06-1989", "31-07-1994")

    assert resultado.is_empty()
    assert resultado.schema == _ESQUEMA


def test_ultimos_respeita_corte_historico():
    resultado = yd.igpm.indice_serie(ultimos=1000)

    assert resultado.schema == _ESQUEMA
    assert resultado["periodo"].min() == _INICIO_COBERTURA
    assert resultado.item(0, "indice").as_tuple() == Decimal("100.000").as_tuple()


@pytest.mark.parametrize("data", [None, "", "01-01-1900", "01-01-2100"])
def test_indice_indisponivel_retorna_decimal_nan(data):
    resultado = yd.igpm.indice(data)

    assert isinstance(resultado, Decimal)
    assert resultado.is_nan()


def test_intervalo_sem_dados_preserva_esquema():
    resultado = yd.igpm.indice_serie("01-01-1900", "31-12-1900")

    assert resultado.is_empty()
    assert resultado.schema == _ESQUEMA


@pytest.mark.parametrize(
    ("parametros", "mensagem"),
    [
        ({"ultimos": 0}, "maior que 0"),
        ({"ultimos": -1}, "maior que 0"),
        ({}, "Informe 'inicio'"),
        ({"fim": "01-07-2000"}, "Informe 'inicio'"),
        ({"inicio": "invalida"}, "Data inválida"),
        ({"inicio": ""}, "datas do intervalo"),
        ({"inicio": "01-07-2000", "fim": ""}, "datas do intervalo"),
    ],
)
def test_validacao_antes_de_buscar(monkeypatch, parametros, mensagem):
    def falhar_fetch():
        raise AssertionError("O histórico não deveria ser buscado.")

    monkeypatch.setattr(igpm, "_buscar_historico", falhar_fetch)

    with pytest.raises(ValueError, match=mensagem):
        yd.igpm.indice_serie(**parametros)


def test_intervalo_invertido_nao_busca(monkeypatch):
    def falhar_fetch():
        raise AssertionError("O histórico não deveria ser buscado.")

    monkeypatch.setattr(igpm, "_buscar_historico", falhar_fetch)

    resultado = yd.igpm.indice_serie("01-07-2000", "30-06-2000")

    assert resultado.is_empty()
    assert resultado.schema == _ESQUEMA


@pytest.mark.parametrize(
    "conteudo",
    [
        b'{"value": []}',
        b'{"value": [{"VALDATA": "2000-06-01T00:00:00-03:00", "VALVALOR": null}]}',
    ],
)
def test_resposta_sem_valores_preserva_esquema(monkeypatch, conteudo):
    monkeypatch.setattr(igpm, "_buscar_historico", lambda: conteudo)

    resultado = yd.igpm.indice_serie(ultimos=1)

    assert resultado.is_empty()
    assert resultado.schema == _ESQUEMA
    assert yd.igpm.indice("01-06-2000").is_nan()
