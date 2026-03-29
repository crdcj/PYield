import datetime as dt
import json
from pathlib import Path

import pyield.b3.intraday_derivatives as derivatives_mod
from pyield.b3.futures import intraday as futures_intraday_mod

DIRETORIO_DADOS = Path(__file__).parent / "data"
DATA_REFERENCIA = dt.date(2026, 3, 10)
HORARIO_REFERENCIA = dt.datetime(2026, 3, 10, 12, 0)
TAMANHO_CODIGO_NEGOCIACAO_FUTURO = 6


def _carregar_json_scty(codigo_contrato: str) -> list[dict]:
    """Carrega a lista Scty do JSON bruto de referência."""
    caminho = DIRETORIO_DADOS / f"intraday_20260310_{codigo_contrato}.json"
    with open(caminho, encoding="utf-8") as f:
        return json.load(f)["Scty"]


def _buscar_json_intraday_mock(codigo_contrato: str) -> list[dict]:
    return _carregar_json_scty(codigo_contrato)


def _data_referencia_mock() -> dt.date:
    return DATA_REFERENCIA


def _horario_referencia_mock() -> dt.datetime:
    return HORARIO_REFERENCIA


def test_fetch_intraday_derivatives_preserva_payload_misto(monkeypatch):
    """Módulo bruto deve preservar mercados mistos do payload do dia."""
    monkeypatch.setattr(
        derivatives_mod,
        "_buscar_json_intraday",
        _buscar_json_intraday_mock,
    )
    monkeypatch.setattr(derivatives_mod, "intraday_disponivel", lambda: True)

    resultado = derivatives_mod.fetch_intraday_derivatives("DOL")
    total_esperado = len(_carregar_json_scty("DOL"))

    assert resultado.height == total_esperado
    assert resultado["codigo_mercado"].unique().sort().to_list() == [
        "FUT",
        "OPTEXER",
        "SOPT",
        "SPOT",
    ]


def test_fetch_intraday_derivatives_suporta_colunas_opcionais_ausentes(monkeypatch):
    """Módulo bruto não deve quebrar quando o payload não tem book de ofertas."""
    monkeypatch.setattr(
        derivatives_mod,
        "_buscar_json_intraday",
        _buscar_json_intraday_mock,
    )
    monkeypatch.setattr(derivatives_mod, "intraday_disponivel", lambda: True)

    resultado = derivatives_mod.fetch_intraday_derivatives("DDI")
    total_esperado = len(_carregar_json_scty("DDI"))

    assert resultado.height == total_esperado
    assert "preco_oferta_compra" not in resultado.columns
    assert "preco_oferta_venda" not in resultado.columns


def test_fetch_intraday_derivatives_nao_descarta_fro_sem_curprc(monkeypatch):
    """FRO deve continuar válido mesmo sem coluna curPrc no payload."""
    monkeypatch.setattr(
        derivatives_mod,
        "_buscar_json_intraday",
        _buscar_json_intraday_mock,
    )
    monkeypatch.setattr(derivatives_mod, "intraday_disponivel", lambda: True)

    resultado = derivatives_mod.fetch_intraday_derivatives("FRO")
    total_esperado = len(_carregar_json_scty("FRO"))

    assert resultado.height == total_esperado
    assert "preco_ultimo" not in resultado.columns
    assert resultado["codigo_mercado"].unique().to_list() == ["FUT"]


def test_futures_intraday_filtra_apenas_futuros(monkeypatch):
    """Camada de futures deve consumir o bruto e manter apenas FUT."""
    monkeypatch.setattr(
        derivatives_mod,
        "_buscar_json_intraday",
        _buscar_json_intraday_mock,
    )
    monkeypatch.setattr(derivatives_mod, "intraday_disponivel", lambda: True)
    monkeypatch.setattr(derivatives_mod.clock, "now", _horario_referencia_mock)
    monkeypatch.setattr(
        futures_intraday_mod,
        "fetch_intraday_derivatives",
        derivatives_mod.fetch_intraday_derivatives,
    )
    monkeypatch.setattr(
        futures_intraday_mod.bday, "last_business_day", _data_referencia_mock
    )

    resultado = futures_intraday_mod.intraday("DOL")

    codigos_fut_esperados = [
        item["symb"]
        for item in _carregar_json_scty("DOL")
        if item.get("mkt", {}).get("cd") == "FUT"
    ]
    codigos_fut_esperados.sort()

    assert resultado.height == len(codigos_fut_esperados)
    assert resultado["codigo_negociacao"].sort().to_list() == codigos_fut_esperados
    assert all(
        len(codigo) == TAMANHO_CODIGO_NEGOCIACAO_FUTURO
        for codigo in resultado["codigo_negociacao"].to_list()
    )
