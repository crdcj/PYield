import datetime as dt

import polars as pl
from polars.testing import assert_frame_equal

import pyield as yd
from pyield.b3.futuro import historico as historico_mod


def test_historico_usa_dataset_pr(monkeypatch):
    """historico() deve retornar dados do cache PR."""

    def _historico_pr_falso(datas, codigo_contrato):
        return pl.DataFrame({"codigo_negociacao": ["DI1N26"]})

    monkeypatch.setattr(
        historico_mod,
        "_buscar_do_cache",
        _historico_pr_falso,
    )

    df = historico_mod.historico(dt.date(2026, 5, 10), "DI1")
    assert not df.is_empty()


def test_historico_retorna_vazio_sem_cache(monkeypatch):
    """historico() retorna vazio quando contrato não está no cache."""
    monkeypatch.setattr(
        historico_mod,
        "_buscar_do_cache",
        lambda datas, codigo_contrato: pl.DataFrame(),
    )

    df = historico_mod.historico(dt.date(2026, 5, 10), "XYZ")
    assert df.is_empty()


def test_futuro_igual_dataset_pr_di1():
    """`futuro` deve bater com o dataset PR na mesma data."""
    data = dt.date(2026, 1, 12)

    df_futuro = yd.futuro(codigo_contrato="DI1", data_referencia=data)
    df_pr = historico_mod._buscar_do_cache([data], "DI1")

    assert_frame_equal(
        df_futuro.sort("data_vencimento"),
        df_pr.sort("data_vencimento"),
        rel_tol=1e-6,
        check_exact=False,
        check_dtypes=True,
    )
