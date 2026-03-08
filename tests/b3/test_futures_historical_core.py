import datetime as dt
import importlib

import polars as pl

historical_mod = importlib.import_module("pyield.b3.futures.historical")


def test_historical_prioriza_dataset_pr(monkeypatch):
    """Quando há dados no cache PR, não deve chamar fallback SPR."""

    def _historico_pr_falso(data, codigo_contrato):
        return pl.DataFrame({"TickerSymbol": ["DI1N26"]})

    chamou_price_report = {"valor": False}

    def _price_report_falso(date, contract_code, source_type):
        chamou_price_report["valor"] = True
        return pl.DataFrame()

    monkeypatch.setattr(
        historical_mod,
        "_buscar_df_historico_dataset_pr",
        _historico_pr_falso,
    )
    monkeypatch.setattr(historical_mod, "fetch_price_report", _price_report_falso)

    df = historical_mod.historical(dt.date(2026, 5, 10), "DI1")

    assert not df.is_empty()
    assert not chamou_price_report["valor"]


def test_historical_faz_fallback_para_price_report(monkeypatch):
    """Se cache PR vier vazio, deve fazer fallback para SPR."""
    monkeypatch.setattr(
        historical_mod,
        "_buscar_df_historico_dataset_pr",
        lambda data, codigo_contrato: pl.DataFrame(),
    )

    chamadas_price_report = []

    def _price_report_falso(date, contract_code, source_type):
        chamadas_price_report.append(source_type)
        return pl.DataFrame({"TickerSymbol": ["DI1N26"]})

    monkeypatch.setattr(historical_mod, "fetch_price_report", _price_report_falso)

    df = historical_mod.historical(dt.date(2026, 5, 10), "DI1")

    assert chamadas_price_report == ["SPR"]
    assert not df.is_empty()
