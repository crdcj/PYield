"""Testes do pipeline bruto (raw) do price_report.

Valida que o parsing XML → tipagem → renomeação de colunas produz o
DataFrame esperado, **sem** enriquecimento (ExpirationDate, BDaysToExp,
DaysToExp, DV01, ForwardRate, etc.).

Os parquets de referência foram gerados a partir do ZIP PR260112.zip
usando as funções internas do módulo.
"""

import importlib
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

pr_mod = importlib.import_module("pyield.b3.price_report")

TEST_DATA_DIR = Path(__file__).parent / "data"
ZIP_PR = TEST_DATA_DIR / "PR260112.zip"


def _processar_bruto(zip_path: Path, contract_code: str) -> pl.DataFrame:
    """Pipeline bruto: ZIP → XML → parse → tipos → renomeação."""
    dados_zip = pr_mod._ler_zip_arquivo(zip_path)
    xml_bytes = pr_mod._extrair_xml_zip_aninhado(dados_zip)
    registros = pr_mod._parsear_xml_registros(xml_bytes, contract_code)
    if not registros:
        return pl.DataFrame()
    df = pr_mod._converter_para_df(registros)
    mapa = pr_mod._mapa_renomeacao_colunas(contract_code)
    df = df.rename(mapa, strict=False)
    return df.sort("TickerSymbol")


def _parquet_referencia(contract_code: str) -> Path:
    return TEST_DATA_DIR / f"price_report_20260112_{contract_code}.parquet"


@pytest.mark.parametrize(
    "contract_code",
    ["DI1", "FRC", "DDI", "DAP", "DOL", "WDO", "IND", "WIN"],
)
def test_pipeline_bruto_price_report(contract_code: str):
    """Compara saída bruta do price_report com parquet canônico."""
    df_result = _processar_bruto(ZIP_PR, contract_code)
    df_expect = pl.read_parquet(_parquet_referencia(contract_code))

    assert not df_result.is_empty(), f"Resultado vazio para {contract_code}"
    assert_frame_equal(df_result, df_expect, check_exact=True, check_dtypes=True)
