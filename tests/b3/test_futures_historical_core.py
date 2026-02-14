import datetime as dt
import importlib

import polars as pl

hcore = importlib.import_module("pyield.b3.futures.historical.core")


def test_data_antiga_usa_apenas_price_report(monkeypatch):
    """Datas antigas devem ignorar endpoint curto e usar price report."""
    monkeypatch.setattr(hcore.clock, "today", lambda: dt.date(2026, 5, 15))

    hb3_chamou = {"valor": False}

    def _hb3_falso(data, codigo_contrato):
        hb3_chamou["valor"] = True
        return pl.DataFrame()

    chamadas_price_report = []

    def _price_report_falso(date, contract_code, source_type):
        chamadas_price_report.append((date, contract_code, source_type))
        if source_type == "SPR":
            return pl.DataFrame({"TickerSymbol": ["DI1F26"]})
        return pl.DataFrame()

    monkeypatch.setattr(hcore.hb3, "buscar_df_historico", _hb3_falso)
    monkeypatch.setattr(hcore, "fetch_price_report", _price_report_falso)

    df = hcore.buscar_df_historico(dt.date(2026, 1, 15), "DI1")

    assert not hb3_chamou["valor"]
    assert chamadas_price_report == [(dt.date(2026, 1, 15), "DI1", "SPR")]
    assert not df.is_empty()


def test_data_recente_prioriza_endpoint_curto(monkeypatch):
    """Datas recentes usam endpoint curto quando ele retorna dados."""
    monkeypatch.setattr(hcore.clock, "today", lambda: dt.date(2026, 5, 15))

    def _hb3_falso(data, codigo_contrato):
        return pl.DataFrame({"TickerSymbol": ["DI1N26"]})

    chamou_price_report = {"valor": False}

    def _price_report_falso(date, contract_code, source_type):
        chamou_price_report["valor"] = True
        return pl.DataFrame()

    monkeypatch.setattr(hcore.hb3, "buscar_df_historico", _hb3_falso)
    monkeypatch.setattr(hcore, "fetch_price_report", _price_report_falso)

    df = hcore.buscar_df_historico(dt.date(2026, 5, 10), "DI1")

    assert not df.is_empty()
    assert not chamou_price_report["valor"]


def test_data_recente_faz_fallback_para_price_report(monkeypatch):
    """Se endpoint curto vier vazio, deve fazer fallback para SPR."""
    monkeypatch.setattr(hcore.clock, "today", lambda: dt.date(2026, 5, 15))
    monkeypatch.setattr(
        hcore.hb3, "buscar_df_historico", lambda data, codigo_contrato: pl.DataFrame()
    )

    chamadas_price_report = []

    def _price_report_falso(date, contract_code, source_type):
        chamadas_price_report.append(source_type)
        return pl.DataFrame({"TickerSymbol": ["DI1N26"]})

    monkeypatch.setattr(hcore, "fetch_price_report", _price_report_falso)

    df = hcore.buscar_df_historico(dt.date(2026, 5, 10), "DI1")

    assert chamadas_price_report == ["SPR"]
    assert not df.is_empty()
