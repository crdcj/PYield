import datetime as dt
import importlib

import polars as pl
from polars.testing import assert_frame_equal

import pyield as yd

modulo_historico = importlib.import_module("pyield.b3.futuro.historico")


def test_historico_usa_dataset_pr(monkeypatch):
    """historico() deve retornar dados do cache PR."""

    def _historico_pr_falso(datas, contrato):
        return pl.DataFrame({"codigo_negociacao": ["DI1N26"]})

    monkeypatch.setattr(
        modulo_historico,
        "_buscar_do_cache",
        _historico_pr_falso,
    )

    df_resultado = modulo_historico.historico(dt.date(2026, 3, 10), "DI1")
    assert not df_resultado.is_empty()


def test_historico_retorna_vazio_sem_cache(monkeypatch):
    """historico() retorna vazio quando contrato não está no cache."""
    monkeypatch.setattr(
        modulo_historico,
        "_buscar_do_cache",
        lambda datas, contrato: pl.DataFrame(),
    )

    df_resultado = modulo_historico.historico(dt.date(2026, 5, 10), "XYZ")
    assert df_resultado.is_empty()


def test_futuro_igual_dataset_pr_di1():
    """`futuro` deve bater com o dataset PR na mesma data."""
    data = dt.date(2026, 1, 12)

    df_futuro = yd.futuro.historico(data=data, contrato="DI1")
    df_referencia = modulo_historico._buscar_do_cache([data], "DI1")

    assert_frame_equal(
        df_futuro.sort("data_vencimento"),
        df_referencia.sort("data_vencimento"),
        rel_tol=1e-6,
        check_exact=False,
        check_dtypes=True,
    )
