"""Fluxo público de VNA com as planilhas reais do Tesouro Nacional."""

import datetime as dt
from decimal import Decimal, localcontext
from pathlib import Path

import polars as pl
import pytest

import pyield as yd
from pyield._internal.cache import ttl_cache  # noqa: PLC2701
from pyield.ipca import historico as _ipca
from pyield.vna import _download, _ntnb, _ntnc  # noqa: PLC2701

DIRETORIO_DADOS = Path(__file__).parent / "data"
ARQUIVOS = {
    _ntnb._URL_ARQUIVO: "DP_Valores_Nominais_NTNB.xlsx",
    _ntnc._URL_ARQUIVO: "DP_Valores_Nominais_NTNC.xlsx",
}


@pytest.fixture(autouse=True)
def planilhas_locais(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        _download, "baixar_planilha", ttl_cache()(_download.baixar_planilha.__wrapped__)
    )
    monkeypatch.setattr(
        _download,
        "_buscar_conteudo",
        lambda url: (DIRETORIO_DADOS / ARQUIVOS[url]).read_bytes(),
    )


@pytest.mark.parametrize(
    ("titulo", "referencia"),
    [("NTN-B", "vna_ntnb.parquet"), ("NTN-C", "vna_ntnc.parquet")],
)
def test_historico_reproduz_referencia(titulo: str, referencia: str) -> None:
    esperado = pl.read_parquet(DIRETORIO_DADOS / referencia)

    assert yd.vna.historico(titulo).equals(esperado)


@pytest.mark.parametrize(
    ("titulo", "vencimento", "dia"),
    [("NTN-B", None, 15), ("NTN-C", "01-01-2031", 1), ("NTN-C", "01-01-2006", 1)],
)
def test_historico_nao_tem_lacunas_mensais(
    titulo: str, vencimento: str | None, dia: int
) -> None:
    datas = yd.vna.historico(titulo, vencimento)["data"]
    esperado = pl.date_range(datas.min(), datas.max(), "1mo", eager=True)

    assert datas.dt.day().unique().to_list() == [dia]
    assert datas.equals(esperado, check_names=False)


def test_ntnc_le_celula_de_texto_brasileiro() -> None:
    # Na planilha, B223 (01/01/2018) é texto: "\xa03.580,282456".
    assert yd.vna.valor("NTN-C", dt.date(2018, 1, 1), "01-01-2031") == Decimal(
        "3580.282456"
    )


def test_vna_ntnb_independe_da_precisao_externa(monkeypatch) -> None:
    monkeypatch.setattr(
        _ipca,
        "_buscar_dados_api",
        lambda url: {"202511": "7378.94", "202512": "7403.29"},
    )

    with localcontext(prec=9):
        assert yd.vna.valor("NTN-B", "30-12-2025") == Decimal("4577.369436")
