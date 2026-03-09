import datetime as dt
import importlib

import polars as pl
from polars.testing import assert_frame_equal

import pyield as yd

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
        "_carregar_pr_por_data",
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
        "_carregar_pr_por_data",
        lambda data, codigo_contrato: pl.DataFrame(),
    )

    chamadas_price_report = []

    def _price_report_falso(date, contract_code, source_type):
        chamadas_price_report.append(source_type)
        return pl.DataFrame(
            {
                "TickerSymbol": ["DI1N26"],
            }
        )

    monkeypatch.setattr(historical_mod, "fetch_price_report", _price_report_falso)
    # Bypass do enriquecimento para testar apenas o roteamento
    monkeypatch.setattr(historical_mod, "_enriquecer_dados", lambda df, cc: df)
    monkeypatch.setattr(historical_mod, "_selecionar_colunas_saida", lambda df: df)

    df = historical_mod.historical(dt.date(2026, 5, 10), "DI1")

    assert chamadas_price_report == ["SPR"]
    assert not df.is_empty()


def test_futures_igual_price_report_release_di1():
    """`futures` deve bater com o PR remoto do release na mesma data."""
    data = dt.date(2026, 1, 12)

    df_futures = yd.futures(contract_code="DI1", date=data)
    df_price_report = historical_mod.carregar_pr([data], "DI1")

    assert not df_futures.is_empty()
    assert not df_price_report.is_empty()

    assert_frame_equal(
        df_futures.sort("ExpirationDate"),
        df_price_report.sort("ExpirationDate"),
        rel_tol=1e-6,
        check_exact=False,
        check_dtypes=True,
    )
