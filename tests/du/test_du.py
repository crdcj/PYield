# pyright: reportGeneralTypeIssues=false

import datetime as dt

from pyield import du


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
