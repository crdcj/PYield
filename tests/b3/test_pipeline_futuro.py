"""Histórico público de futuros com snapshots locais do schema bruto da B3.

O cache é substituído pelos Parquets brutos, validados separadamente contra
os XMLs no teste do boletim. Conversões de data, filtros, enriquecimento e
seleção das colunas públicas executam normalmente.
"""

import datetime as dt
import importlib
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pyield import futuro

TEST_DATA_DIR = Path(__file__).parent / "data"
modulo_historico = importlib.import_module("pyield.futuro.historico")


@pytest.fixture(scope="module")
def historico_bruto():
    # Várias datas e famílias para exercitar também os filtros da consulta.
    return pl.read_parquet(TEST_DATA_DIR / "boletim_negociacao_*.parquet")


def _parquet_referencia(data: str, contrato: str) -> Path:
    """Retorna o caminho do parquet canônico para a data e contrato."""
    dia, mes, ano = data.split("-")
    nome = f"futuro_{ano}{mes}{dia}_{contrato}.parquet"
    return TEST_DATA_DIR / nome


@pytest.mark.parametrize(
    ("data", "contrato"),
    [
        ("02-02-2023", "DI1"),
        ("02-02-2023", "FRC"),
        ("02-02-2023", "DDI"),
        ("02-02-2023", "DAP"),
        ("02-02-2023", "DOL"),
        ("02-02-2023", "WDO"),
        ("02-02-2023", "IND"),
        ("02-02-2023", "WIN"),
        ("03-02-2025", "DI1"),
        ("03-02-2025", "FRC"),
        ("03-02-2025", "DDI"),
        ("03-02-2025", "DAP"),
        ("03-02-2025", "DOL"),
        ("03-02-2025", "WDO"),
        ("03-02-2025", "IND"),
        ("03-02-2025", "WIN"),
        ("12-01-2026", "DI1"),
        ("12-01-2026", "FRC"),
        ("12-01-2026", "DDI"),
        ("12-01-2026", "DAP"),
        ("12-01-2026", "DOL"),
        ("12-01-2026", "WDO"),
        ("12-01-2026", "IND"),
        ("12-01-2026", "WIN"),
    ],
)
def test_pipeline_futuro(monkeypatch, historico_bruto, data, contrato):
    monkeypatch.setattr(
        modulo_historico, "obter_dataset_cacheado", lambda _: historico_bruto
    )
    df_esperado = pl.read_parquet(_parquet_referencia(data, contrato))

    df_resultado = futuro.historico(data=data, contrato=contrato)

    # Preserva o contrato anterior: as colunas canônicas são obrigatórias;
    # colunas adicionais não interferem na comparação.
    df_resultado = df_resultado.select(df_esperado.columns)
    assert_frame_equal(
        df_resultado,
        df_esperado,
        rel_tol=1e-4,
        check_exact=False,
        check_dtypes=True,
    )


def test_vencimento_aceita_codigo_nulo():
    assert futuro.vencimento(None, "DI1") is None
    assert futuro.vencimento([None, "DI1F27"], "DI1").to_list() == [
        None,
        dt.date(2027, 1, 4),
    ]


def test_historico_nao_mistura_contratos_por_prefixo_parcial(historico_bruto):
    """O contrato é o prefixo completo de três caracteres do ticker."""
    assert futuro.enriquecer(historico_bruto, "D").is_empty()
