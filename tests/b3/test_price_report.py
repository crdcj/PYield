"""Testes do pipeline bruto (raw) do price_report.

Valida que o parsing XML → tipagem → renomeação de colunas produz o
DataFrame esperado, **sem** enriquecimento (ExpirationDate, BDaysToExp,
DaysToExp, DV01, ForwardRate, etc.).

Os parquets de referência são comparados com o XML remoto em .zst
publicado no release de dados de teste.
"""

import importlib
from pathlib import Path

import polars as pl
import pytest
import requests
from compression import zstd
from polars.testing import assert_frame_equal

pr_mod = importlib.import_module("pyield.b3.price_report")

TEST_DATA_DIR = Path(__file__).parent / "data"
DATA_REFERENCIA = "12-01-2026"
URL_BASE_RELEASE = "https://github.com/crdcj/PYield/releases/download/test-data-v1.0"


def _baixar_xml_remoto(date_str: str) -> bytes:
    """Baixa e descompacta o arquivo XML.ZST remoto para a data informada."""
    dia, mes, ano = date_str.split("-")
    arquivo = f"PR{ano[2:]}{mes}{dia}.xml.zst"
    url = f"{URL_BASE_RELEASE}/{arquivo}"

    resposta = requests.get(url, timeout=(5, 30))
    resposta.raise_for_status()
    return zstd.decompress(resposta.content)


def _processar_bruto(xml_bytes: bytes, contract_code: str) -> pl.DataFrame:
    """Pipeline bruto: XML remoto .zst → parse → tipos → renomeação."""
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
    xml_bytes = _baixar_xml_remoto(DATA_REFERENCIA)
    df_result = _processar_bruto(xml_bytes, contract_code)
    df_expect = pl.read_parquet(_parquet_referencia(contract_code))

    assert not df_result.is_empty(), f"Resultado vazio para {contract_code}"
    assert_frame_equal(df_result, df_expect, check_exact=True, check_dtypes=True)
