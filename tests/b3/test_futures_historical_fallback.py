import datetime as dt

import polars as pl
from polars.testing import assert_frame_equal

import pyield as yd
from pyield.b3.futures import historical as historical_mod


def test_historical_prioriza_dataset_pr(monkeypatch):
    """Quando há dados no cache PR, não deve chamar fallback SPR."""

    def _historico_pr_falso(datas, codigo_contrato):
        return pl.DataFrame({"TickerSymbol": ["DI1N26"]})

    chamou_price_report = {"valor": False}

    def _price_report_falso(date, contract_code, full_report):
        chamou_price_report["valor"] = True
        return pl.DataFrame()

    monkeypatch.setattr(
        historical_mod,
        "_obter_futuros_pr",
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
        "_obter_futuros_pr",
        lambda datas, codigo_contrato: pl.DataFrame(),
    )

    chamadas_price_report = []

    def _price_report_falso(date, contract_code, full_report):
        chamadas_price_report.append(full_report)
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

    assert chamadas_price_report == [False]
    assert not df.is_empty()


def test_futures_igual_price_report_release_di1():
    """`futures` deve bater com o PR remoto do release na mesma data."""
    data = dt.date(2026, 1, 12)

    df_futures = yd.futures(contract_code="DI1", date=data)
    df_price_report = historical_mod._obter_futuros_pr([data], "DI1")

    assert not df_futures.is_empty()
    assert not df_price_report.is_empty()

    assert_frame_equal(
        df_futures.sort("ExpirationDate"),
        df_price_report.sort("ExpirationDate"),
        rel_tol=1e-6,
        check_exact=False,
        check_dtypes=True,
    )


def test_historical_lista_contratos_faz_um_fetch_remoto(monkeypatch):
    monkeypatch.setattr(
        historical_mod,
        "_obter_futuros_pr",
        lambda datas, codigo_contrato: pl.DataFrame(),
    )

    chamadas = []

    def _price_report_falso(date, contract_code, full_report):
        chamadas.append((date, contract_code, full_report))
        ticker = "DI1F26" if contract_code == "DI1" else "DOLF26"
        return pl.DataFrame(
            {
                "TickerSymbol": [ticker],
            }
        )

    monkeypatch.setattr(historical_mod, "fetch_price_report", _price_report_falso)
    monkeypatch.setattr(historical_mod, "adicionar_vencimento", lambda df, *_: df)
    monkeypatch.setattr(historical_mod, "_enriquecer_dados", lambda df, cc: df)
    monkeypatch.setattr(historical_mod, "_selecionar_colunas_saida", lambda df: df)

    df = historical_mod.historical(
        dt.date(2026, 5, 10),
        ["DI1", "DOL"],
    )

    assert len(chamadas) == 2  # noqa
    assert sorted(chamada[1] for chamada in chamadas) == ["DI1", "DOL"]
    assert sorted(df.get_column("TickerSymbol").to_list()) == ["DI1F26", "DOLF26"]
