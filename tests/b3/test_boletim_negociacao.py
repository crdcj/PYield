"""Testes do pipeline bruto (raw) do boletim de negociacao.

Valida que o parsing XML → tipagem → renomeação de colunas produz o
DataFrame esperado, **sem** enriquecimento (ExpirationDate, BDaysToExp,
DaysToExp, DV01, ForwardRate, etc.).

Os parquets de referência são comparados com o XML remoto em .gz
publicado no release de dados de teste.
"""

import datetime as dt
import gzip
import io
import zipfile
from functools import lru_cache
from pathlib import Path

import polars as pl
import pytest
import requests
from polars.testing import assert_frame_equal

import pyield as yd
import pyield.b3.boletim as modulo_boletim

TEST_DATA_DIR = Path(__file__).parent / "data"
URL_BASE_RELEASE = "https://github.com/crdcj/PYield/releases/download/test-data"


def test_boletim_api_publica():
    assert yd.b3.boletim is modulo_boletim
    assert yd.b3.boletim.buscar is modulo_boletim.buscar
    assert yd.b3.boletim.ler is modulo_boletim.ler
    assert yd.b3.boletim.extrair is modulo_boletim.extrair
    assert yd.b3.boletim.__all__ == ["buscar", "extrair", "ler"]


def _criar_zip_boletim(nome_xml: str = "PR260112.xml") -> bytes:
    zip_interno_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_interno_bytes, "w") as zip_interno:
        zip_interno.writestr(nome_xml, b"<BizData />")

    zip_externo_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_externo_bytes, "w") as zip_externo:
        zip_externo.writestr("PR260112.zip", zip_interno_bytes.getvalue())
        zip_externo.writestr("padding.bin", b"x" * modulo_boletim.MIN_TAMANHO_ZIP_BYTES)
    return zip_externo_bytes.getvalue()


def test_zip_valido_reconhece_zip_aninhado_com_xml():
    assert modulo_boletim._zip_valido(_criar_zip_boletim())


def test_zip_valido_rejeita_conteudo_pequeno_ou_ilegivel():
    assert not modulo_boletim._zip_valido(b"")
    assert not modulo_boletim._zip_valido(b"x" * modulo_boletim.MIN_TAMANHO_ZIP_BYTES)


def test_zip_valido_rejeita_zip_interno_sem_xml():
    assert not modulo_boletim._zip_valido(_criar_zip_boletim(nome_xml="dados.txt"))


def test_baixar_zip_url_descarta_zip_invalido(monkeypatch):
    class Resposta:
        content = b"x" * modulo_boletim.MIN_TAMANHO_ZIP_BYTES

        def raise_for_status(self):
            pass

    monkeypatch.setattr(modulo_boletim._SESSAO, "get", lambda *_args, **_kwargs: Resposta())

    resultado = modulo_boletim._baixar_zip_url(dt.date(2026, 1, 13), False)

    assert resultado == b""


@lru_cache(maxsize=8)
def _baixar_xml_remoto(data: str) -> bytes:
    """Baixa e descompacta o arquivo XML.GZ remoto para a data informada."""
    dia, mes, ano = data.split("-")
    arquivo = f"PR{ano[2:]}{mes}{dia}.xml.gz"
    url = f"{URL_BASE_RELEASE}/{arquivo}"

    resposta = requests.get(url, timeout=(5, 30))
    resposta.raise_for_status()
    return gzip.decompress(resposta.content)


@lru_cache(maxsize=8)
def _baixar_e_parsear_xml_remoto(data: str) -> pl.DataFrame:
    """Baixa o XML remoto e parseia em DataFrame completo."""
    xml_bytes = _baixar_xml_remoto(data)
    return modulo_boletim.ler(xml_bytes)


def _parquet_referencia(data: str, contrato: str) -> Path:
    dia, mes, ano = data.split("-")
    return TEST_DATA_DIR / f"boletim_negociacao_{ano}{mes}{dia}_{contrato}.parquet"


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
def test_pipeline_bruto_boletim(data: str, contrato: str):
    """Compara saída bruta do boletim de negociacao com parquet canônico."""
    df_completo = _baixar_e_parsear_xml_remoto(data)
    df_resultado = modulo_boletim._filtrar_df(
        df_completo, [contrato], comprimento_ticker=6
    )
    df_esperado = pl.read_parquet(_parquet_referencia(data, contrato))

    assert_frame_equal(df_resultado, df_esperado, check_exact=True, check_dtypes=True)


def test_boletim_reusa_download_xml_por_data(monkeypatch):
    chamadas = {"download": 0, "extrair": 0}

    monkeypatch.setattr(modulo_boletim, "data_negociacao_valida", lambda *_: True)

    def _baixar_zip_falso(*_):
        chamadas["download"] += 1
        return b"zip"

    def _extrair_xml_falso(*_):
        chamadas["extrair"] += 1
        return b"xml"

    def _processar_xml_falso(_xml):
        return pl.DataFrame(
            {
                "TckrSymb": ["DI1F26", "DOLF26"],
                "TradDt": [dt.date(2026, 1, 12)] * 2,
            }
        ).cast({"TradDt": pl.Date})

    monkeypatch.setattr(modulo_boletim, "_baixar_zip_url", _baixar_zip_falso)
    monkeypatch.setattr(modulo_boletim, "extrair", _extrair_xml_falso)
    monkeypatch.setattr(modulo_boletim, "_processar_xml_extraido", _processar_xml_falso)

    _ = modulo_boletim.buscar(data="12-01-2026", prefixo_ticker="DI1")
    _ = modulo_boletim.buscar(data="12-01-2026", prefixo_ticker="DOL")

    # Monkeypatch substitui a função cacheada, então cada chamada passa direto
    assert chamadas == {"download": 2, "extrair": 2}


def test_filtrar_df_trata_prefixo_como_literal():
    df = pl.DataFrame(
        {
            "TckrSymb": ["D.F26", "DAPF26", "DDIF26"],
            "TradDt": [dt.date(2026, 1, 12)] * 3,
        }
    ).cast({"TradDt": pl.Date})

    df_resultado = modulo_boletim._filtrar_df(df, ["D."])
    df_esperado = pl.DataFrame(
        {
            "TckrSymb": ["D.F26"],
            "TradDt": [dt.date(2026, 1, 12)],
        }
    ).cast({"TradDt": pl.Date})

    assert_frame_equal(df_resultado, df_esperado, check_exact=True, check_dtypes=True)
