"""Testes do pipeline bruto (raw) do price_report.

Valida que o parsing XML → tipagem → renomeação de colunas produz o
DataFrame esperado, **sem** enriquecimento (ExpirationDate, BDaysToExp,
DaysToExp, DV01, ForwardRate, etc.).

Os parquets de referência são comparados com o XML remoto em .gz
publicado no release de dados de teste.
"""

import datetime as dt
import gzip
from functools import lru_cache
from pathlib import Path

import polars as pl
import pytest
import requests
from polars.testing import assert_frame_equal

import pyield.b3.price_report as pr_mod

TEST_DATA_DIR = Path(__file__).parent / "data"
URL_BASE_RELEASE = "https://github.com/crdcj/PYield/releases/download/test-data"


@lru_cache(maxsize=8)
def _baixar_xml_remoto(date_str: str) -> bytes:
    """Baixa e descompacta o arquivo XML.GZ remoto para a data informada."""
    dia, mes, ano = date_str.split("-")
    arquivo = f"PR{ano[2:]}{mes}{dia}.xml.gz"
    url = f"{URL_BASE_RELEASE}/{arquivo}"

    resposta = requests.get(url, timeout=(5, 30))
    resposta.raise_for_status()
    return gzip.decompress(resposta.content)


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
    df_result = pr_mod.price_report_read(xml_bytes, contract_code)
    df_expect = pl.read_parquet(_parquet_referencia(date, contract_code))

    assert_frame_equal(df_result, df_expect, check_exact=True, check_dtypes=True)


def test_price_report_fetch_reusa_download_xml_por_data(monkeypatch):
    pr_mod._obter_xml_price_report.cache_clear()

    chamadas = {"download": 0, "extrair": 0}

    monkeypatch.setattr(pr_mod, "data_negociacao_valida", lambda *_: True)

    def _baixar_zip_falso(*_):
        chamadas["download"] += 1
        return b"zip"

    def _extrair_xml_falso(*_):
        chamadas["extrair"] += 1
        return b"xml"

    def _processar_xml_falso(_xml, codigo):
        return pl.DataFrame(
            {
                "TckrSymb": [f"{codigo}F26"],
                "TradDt": [dt.date(2026, 1, 12)],
            }
        )

    monkeypatch.setattr(pr_mod, "_baixar_zip_url", _baixar_zip_falso)
    monkeypatch.setattr(pr_mod, "price_report_extract", _extrair_xml_falso)
    monkeypatch.setattr(pr_mod, "_processar_xml_extraido", _processar_xml_falso)

    _ = pr_mod.price_report_fetch(date="12-01-2026", contract_code="DI1")
    _ = pr_mod.price_report_fetch(date="12-01-2026", contract_code="DOL")

    assert chamadas == {"download": 1, "extrair": 1}

    pr_mod._obter_xml_price_report.cache_clear()
