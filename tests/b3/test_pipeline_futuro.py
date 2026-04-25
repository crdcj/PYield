"""Testes do pipeline completo de futuro (dado enriquecido).

Valida que o fluxo end-to-end (PR remoto no release em .gz →
tipagem/renomeação → data_vencimento → dias_uteis/dias_corridos/dv01/taxa_forward →
seleção de colunas) produz o DataFrame esperado.

Estratégia: exercita o pipeline real completo em rede, sem fixtures de ZIP local.
"""

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pyield import futuro

TEST_DATA_DIR = Path(__file__).parent / "data"


def _parquet_referencia(data: str, contrato: str) -> Path:
    """Retorna o caminho do parquet canônico para a data e contrato."""
    dia, mes, ano = data.split("-")
    nome = f"futuro_{ano}{mes}{dia}_{contrato}.parquet"
    return TEST_DATA_DIR / nome


def _preparar(
    data: str,
    contrato: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Executa o pipeline real completo usando dataset PR remoto do release."""
    df_esperado = pl.read_parquet(_parquet_referencia(data, contrato))

    df_resultado = futuro.historico(data=data, contrato=contrato)
    return df_resultado, df_esperado


def _alinhar_colunas(
    df_resultado: pl.DataFrame,
    df_esperado: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Alinha DataFrames para comparação usando o canônico como subconjunto obrigatório."""
    colunas_esperadas = df_esperado.columns
    colunas_faltantes = [
        col for col in colunas_esperadas if col not in df_resultado.columns
    ]
    if colunas_faltantes:
        raise AssertionError(
            f"Colunas esperadas ausentes no resultado: {colunas_faltantes}"
        )

    return df_resultado.select(colunas_esperadas), df_esperado


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
def test_pipeline_futuro(data, contrato):
    """Compara `futuro` com parquet canônico usando dados remotos do release."""
    df_resultado, df_esperado = _preparar(data, contrato)
    df_resultado, df_esperado = _alinhar_colunas(df_resultado, df_esperado)
    assert_frame_equal(
        df_resultado,
        df_esperado,
        rel_tol=1e-4,
        check_exact=False,
        check_dtypes=True,
    )
