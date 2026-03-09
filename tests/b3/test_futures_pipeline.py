"""Testes do pipeline completo de futures (dado enriquecido).

Valida que o fluxo end-to-end (PR remoto no release em .zst →
tipagem/renomeação → ExpirationDate → BDaysToExp/DaysToExp/DV01/ForwardRate →
seleção de colunas) produz o DataFrame esperado.

Estratégia: exercita o pipeline real completo em rede, sem fixtures de ZIP local.
"""

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pyield import b3

TEST_DATA_DIR = Path(__file__).parent / "data"


def _parquet_referencia(date_str: str, contract_code: str) -> Path:
    """Retorna o caminho do parquet canônico para a data e contrato."""
    dia, mes, ano = date_str.split("-")
    nome = f"futures_{ano}{mes}{dia}_{contract_code}.parquet"
    return TEST_DATA_DIR / nome


def _preparar(
    date_str: str,
    contract_code: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Executa o pipeline real completo usando dataset PR remoto do release."""
    df_expect = pl.read_parquet(_parquet_referencia(date_str, contract_code))

    df_result = b3.futures(contract_code=contract_code, date=date_str)
    return df_result, df_expect


def _alinhar_colunas(
    df_result: pl.DataFrame,
    df_expect: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Alinha DataFrames para comparação usando o canônico como subconjunto obrigatório."""
    colunas_esperadas = df_expect.columns
    colunas_faltantes = [
        col for col in colunas_esperadas if col not in df_result.columns
    ]
    if colunas_faltantes:
        raise AssertionError(
            f"Colunas esperadas ausentes no resultado: {colunas_faltantes}"
        )

    return df_result.select(colunas_esperadas), df_expect


@pytest.mark.parametrize(
    ("date", "contract_code"),
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
def test_pipeline_futures(date, contract_code):
    """Compara `futures` com parquet canônico usando dados remotos do release."""
    result_df, expect_df = _preparar(date, contract_code)
    result_df, expect_df = _alinhar_colunas(result_df, expect_df)
    assert_frame_equal(
        result_df, expect_df, rel_tol=1e-4, check_exact=False, check_dtypes=True
    )
