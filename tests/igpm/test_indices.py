"""Consulta pública dos números-índice com JSON real e VNAs oficiais."""

import datetime as dt
from decimal import ROUND_DOWN, Decimal, localcontext
from pathlib import Path

import polars as pl
import pytest

import pyield as yd
from pyield._internal import igpm  # noqa: PLC2701

_DADOS = Path(__file__).parent / "data" / "igpm_ipeadata_202609.json"
_ESQUEMA = {"periodo": pl.Int64, "indice": pl.Decimal(precision=18, scale=10)}


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
    assert resultado.as_tuple() == Decimal("183.7450000000").as_tuple()


def test_indice_preserva_valor_anterior_a_1994():
    assert yd.igpm.indice("01-06-1989") == Decimal("0.0000308619")


def test_serie_intervalo_inclusivo_por_mes():
    resultado = yd.igpm.indice_serie("15-06-2000", "20-07-2000")

    assert resultado.schema == _ESQUEMA
    assert resultado.to_dicts() == [
        {"periodo": 200006, "indice": Decimal("183.7450000000")},
        {"periodo": 200007, "indice": Decimal("186.6340000000")},
    ]


def test_serie_fim_padrao_usa_hoje(monkeypatch):
    monkeypatch.setattr(igpm.relogio, "hoje", lambda: dt.date(2000, 7, 20))

    resultado = yd.igpm.indice_serie("15-06-2000")

    assert resultado["periodo"].to_list() == [200006, 200007]


def test_ultimos_tem_prioridade():
    resultado = yd.igpm.indice_serie("invalida", "invalida", ultimos=1)

    assert resultado.schema == _ESQUEMA
    assert resultado.to_dicts() == [
        {"periodo": 202609, "indice": Decimal("1225.8460000000")}
    ]


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


@pytest.mark.parametrize(
    ("vencimento", "data_base"),
    [(2031, dt.date(2000, 7, 1)), (2006, dt.date(1999, 12, 3))],
)
def test_indices_reproduzem_todos_os_vnas_oficiais(vencimento, data_base):
    indices = {
        r["periodo"]: r["indice"]
        for r in yd.igpm.indice_serie("01-11-1999", "30-09-2026").to_dicts()
    }
    referencia = pl.read_parquet(
        Path(__file__).parents[1] / "vna" / "data" / "vna_ntnc.parquet"
    ).filter(pl.col("anos_vencimento").list.contains(vencimento))

    with localcontext(prec=28):
        fator_inicial = Decimal(1)
        denominador = indices[200006]
        if data_base.day != 1:
            # A emissão de 03/12/1999 tem 29 dias de atualização em dezembro.
            expoente = (Decimal(29) / 31).quantize(
                Decimal("1e-14"), rounding=ROUND_DOWN
            )
            fator_inicial = (indices[199912] / indices[199911]) ** expoente
            denominador = indices[199912]
        for data, _, esperado in referencia.iter_rows():
            mes = data - dt.timedelta(days=1)
            numerador = indices[mes.year * 100 + mes.month]
            fator = (fator_inicial * numerador / denominador).quantize(
                Decimal("1e-16"), rounding=ROUND_DOWN
            )
            resultado = (Decimal(1000) * fator).quantize(
                Decimal("1e-6"), rounding=ROUND_DOWN
            )

            assert resultado == esperado, data
