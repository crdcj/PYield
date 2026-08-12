import datetime as dt
import importlib

import polars as pl

import pyield as yd

modulo_dealers = importlib.import_module("pyield.tpf.dealers")

DADOS_API = {
    "registros": [
        {
            "INICIO_PERIODO": "2026-08-10",
            "FIM_PERIODO": "2027-01-31",
            "CNPJ": " 02.332.886/0001-04 ",
            "DEALER": " XP Investimentos CCTVM S/A ",
        },
        {
            "INICIO_PERIODO": "2026-02-10",
            "FIM_PERIODO": "2026-07-31",
            "CNPJ": "30.306.294/0001-45",
            "DEALER": "Bco BTG Pactual S A",
        },
        {
            "INICIO_PERIODO": "2026-02-10",
            "FIM_PERIODO": "2026-07-31",
            "CNPJ": "00.000.000/0001-91",
            "DEALER": "001 - Banco do Brasil",
        },
    ],
    "status": "ok",
}


def test_dealers_filtra_periodo_e_expoe_api_publica(monkeypatch):
    monkeypatch.setattr(modulo_dealers, "_buscar_dealers", lambda: DADOS_API)

    resultado = yd.tpf.dealers("15-03-2026")

    esperado = pl.DataFrame(
        {
            "inicio_periodo": [dt.date(2026, 2, 10)] * 2,
            "fim_periodo": [dt.date(2026, 7, 31)] * 2,
            "cnpj": ["00.000.000/0001-91", "30.306.294/0001-45"],
            "instituicao": ["001 - Banco do Brasil", "Bco BTG Pactual S A"],
        },
        schema=modulo_dealers.ESQUEMA_SAIDA,
    )
    assert resultado.equals(esperado)
    assert yd.tpf.dealers is modulo_dealers.dealers


def test_dealers_sem_data_usa_hoje(monkeypatch):
    monkeypatch.setattr(modulo_dealers, "_buscar_dealers", lambda: DADOS_API)
    monkeypatch.setattr(modulo_dealers.relogio, "hoje", lambda: dt.date(2026, 8, 12))

    resultado = yd.tpf.dealers()

    assert resultado["instituicao"].to_list() == ["XP Investimentos CCTVM S/A"]
    assert resultado["cnpj"].to_list() == ["02.332.886/0001-04"]


def test_dealers_sem_registro_retorna_esquema_estavel(monkeypatch):
    monkeypatch.setattr(modulo_dealers, "_buscar_dealers", lambda: DADOS_API)

    resultado = yd.tpf.dealers("01-01-2010")

    assert resultado.is_empty()
    assert resultado.schema == modulo_dealers.ESQUEMA_SAIDA


def test_dealers_resposta_vazia_retorna_esquema_estavel(monkeypatch):
    monkeypatch.setattr(
        modulo_dealers,
        "_buscar_dealers",
        lambda: {"registros": [], "status": "ok"},
    )

    resultado = yd.tpf.dealers("15-03-2026")

    assert resultado.is_empty()
    assert resultado.schema == modulo_dealers.ESQUEMA_SAIDA


def test_dealers_data_vazia_nao_consulta_api(monkeypatch):
    def buscar_dealers():
        raise AssertionError("A API não deveria ser consultada.")

    monkeypatch.setattr(modulo_dealers, "_buscar_dealers", buscar_dealers)

    resultado = yd.tpf.dealers("  ")

    assert resultado.is_empty()
    assert resultado.schema == modulo_dealers.ESQUEMA_SAIDA
