"""Testes do pipeline bruto (raw) do price_report.

Valida que o parsing XML → tipagem → renomeação de colunas produz o
DataFrame esperado, **sem** enriquecimento (ExpirationDate, BDaysToExp,
DaysToExp, DV01, ForwardRate, etc.).

Os parquets de referência são comparados com o XML remoto em .zst
publicado no release de dados de teste.
"""

import importlib
from functools import lru_cache
from pathlib import Path

import polars as pl
import pytest
import requests
from compression import zstd
from polars.testing import assert_frame_equal

pr_mod = importlib.import_module("pyield.b3.price_report")

TEST_DATA_DIR = Path(__file__).parent / "data"
URL_BASE_RELEASE = "https://github.com/crdcj/PYield/releases/download/test-data-v1.0"


@lru_cache(maxsize=8)
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


def _parquet_referencia(date_str: str, contract_code: str) -> Path:
    dia, mes, ano = date_str.split("-")
    return TEST_DATA_DIR / f"price_report_{ano}{mes}{dia}_{contract_code}.parquet"


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
def test_pipeline_bruto_price_report(date: str, contract_code: str):
    """Compara saída bruta do price_report com parquet canônico."""
    xml_bytes = _baixar_xml_remoto(date)
    df_result = _processar_bruto(xml_bytes, contract_code)
    df_expect = pl.read_parquet(_parquet_referencia(date, contract_code))

    assert not df_result.is_empty(), f"Resultado vazio para {contract_code}"
    assert_frame_equal(df_result, df_expect, check_exact=True, check_dtypes=True)
