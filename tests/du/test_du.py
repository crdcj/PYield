# pyright: reportGeneralTypeIssues=false

import datetime as dt

import polars as pl

from pyield import du


def test_contar_novo_feriado():
    inicio = "20-11-2024"  # Wednesday (Zumbi Nacional Day)
    fim = "21-11-2024"
    resultado_esperado = 0
    resultado = du.contar(inicio, fim)
    assert resultado == resultado_esperado, (
        f"Esperado {resultado_esperado}, obtido {resultado}"
    )


def test_contar_feriado_antigo():
    inicio = "20-11-2020"  # Friday (was not a holiday in 2020)
    fim = "21-11-2020"
    resultado_esperado = 1
    resultado = du.contar(inicio, fim)
    assert resultado == resultado_esperado, (
        f"Esperado {resultado_esperado}, obtido {resultado}"
    )


def test_contar_feriados_antigo_e_novo_em_lista():
    inicio = ["20-11-2020", "20-11-2024"]
    fim = ["21-11-2020", "21-11-2024"]
    resultado_esperado = [1, 0]
    resultado = du.contar(inicio, fim)
    assert isinstance(resultado, pl.Series)
    assert resultado.to_list() == resultado_esperado, (
        f"Esperado {resultado_esperado}, obtido {resultado.to_list()}"
    )


def test_deslocar_com_feriado_antigo():
    inicio = "20-11-2020"
    deslocamento = 0
    resultado_esperado = dt.date(2020, 11, 20)
    resultado = du.deslocar(inicio, deslocamento)
    assert resultado == resultado_esperado, (
        f"Esperado {resultado_esperado}, obtido {resultado}"
    )


def test_deslocar_com_novo_feriado():
    inicio = "20-11-2024"
    deslocamento = 0
    resultado_esperado = dt.date(2024, 11, 21)
    resultado = du.deslocar(inicio, deslocamento)
    assert resultado == resultado_esperado, (
        f"Esperado {resultado_esperado}, obtido {resultado}"
    )


def test_deslocar_com_feriados_antigo_e_novo():
    inicio = ["20-11-2020", "20-11-2024"]
    deslocamento = 0
    resultado_esperado = [dt.date(2020, 11, 20), dt.date(2024, 11, 21)]
    resultado = du.deslocar(inicio, deslocamento)
    assert isinstance(resultado, pl.Series)
    # Polars Series of dates returns python date objects in to_list()
    assert resultado.to_list() == resultado_esperado, (
        f"Esperado {resultado_esperado}, obtido {resultado.to_list()}"
    )


def test_gerar_inicio_invalido_retorna_hoje(monkeypatch):
    data_fixa = dt.date(2024, 3, 1)
    monkeypatch.setattr("pyield.du.core.relogio.hoje", lambda: data_fixa)

    resultado = du.gerar(inicio="31-02-2024", fim="04-03-2024")
    esperado = du.gerar(inicio=None, fim="04-03-2024")

    assert resultado.equals(esperado)


def test_gerar_fim_invalido_retorna_hoje(monkeypatch):
    data_fixa = dt.date(2024, 3, 4)
    monkeypatch.setattr("pyield.du.core.relogio.hoje", lambda: data_fixa)

    resultado = du.gerar(inicio="01-03-2024", fim="31-02-2024")
    esperado = du.gerar(inicio="01-03-2024", fim=None)

    assert resultado.equals(esperado)
