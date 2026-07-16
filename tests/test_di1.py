import datetime as dt
import importlib
from typing import Any, cast

import polars as pl
import pytest

from pyield.futuro import di1

tpf_taxas = importlib.import_module("pyield.tpf._taxas")

TAXA_FECHAMENTO_CURTA = 0.11


def _historico_di1_fake(datas: list[dt.date], contrato: str) -> pl.DataFrame:
    assert contrato == "DI1"
    data_ref = dt.date(2025, 1, 2)
    if data_ref not in datas:
        return pl.DataFrame()

    return pl.DataFrame(
        {
            "data_referencia": [data_ref, data_ref],
            "data_vencimento": [dt.date(2025, 1, 3), dt.date(2025, 1, 6)],
            "dias_uteis": [1, 2],
            "taxa_ajuste": [0.10, 0.20],
            "taxa_fechamento": [TAXA_FECHAMENTO_CURTA, 0.21],
        }
    )


def _historico_di1_fechamento_nulo(datas: list[dt.date], contrato: str) -> pl.DataFrame:
    assert contrato == "DI1"
    data_ref = dt.date(2025, 1, 2)
    if data_ref not in datas:
        return pl.DataFrame()

    return pl.DataFrame(
        {
            "data_referencia": [data_ref],
            "data_vencimento": [dt.date(2025, 1, 3)],
            "dias_uteis": [1],
            "taxa_ajuste": [0.10],
            "taxa_fechamento": [None],
        }
    )


def test_interpolar_taxas_usa_taxa_ajuste_por_padrao(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(di1, "buscar_historico_cacheado", _historico_di1_fake)

    taxas = di1.interpolar_taxas("02-01-2025", ["03-01-2025", "06-01-2025"])

    assert taxas.to_list() == [0.10, 0.20]


def test_dados_filtra_pre_sem_acessar_cache_tpf_diretamente(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(di1, "buscar_historico_cacheado", _historico_di1_fake)
    monkeypatch.setattr(
        tpf_taxas,
        "_vencimentos_historicos",
        lambda _: pl.DataFrame(
            {
                "data_referencia": [dt.date(2024, 12, 31)],
                "data_vencimento": [dt.date(2025, 1, 6)],
            }
        ),
    )

    resultado = di1.dados("02-01-2025", filtrar_pre=True)

    assert resultado["data_vencimento"].to_list() == [dt.date(2025, 1, 6)]


def test_interpolar_taxas_permite_taxa_fechamento(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(di1, "buscar_historico_cacheado", _historico_di1_fake)

    taxas = di1.interpolar_taxas(
        "02-01-2025",
        ["03-01-2025", "06-01-2025"],
        tipo_taxa="fechamento",
    )

    assert taxas.to_list() == [0.11, 0.21]


def test_interpolar_taxas_retorna_nulo_quando_fechamento_nao_tem_vertice(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(di1, "buscar_historico_cacheado", _historico_di1_fechamento_nulo)

    taxas = di1.interpolar_taxas(
        "02-01-2025",
        "03-01-2025",
        tipo_taxa="fechamento",
    )

    assert taxas.to_list() == [None]


def test_interpolar_taxas_rejeita_tipo_taxa_invalido():
    with pytest.raises(ValueError, match="tipo_taxa"):
        di1.interpolar_taxas(
            "02-01-2025",
            "03-01-2025",
            tipo_taxa=cast(Any, "media"),
        )


def test_interpolar_taxa_repassa_tipo_taxa(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(di1, "buscar_historico_cacheado", _historico_di1_fake)

    taxa = di1.interpolar_taxa(
        "02-01-2025",
        "03-01-2025",
        tipo_taxa="fechamento",
    )

    assert taxa == TAXA_FECHAMENTO_CURTA
