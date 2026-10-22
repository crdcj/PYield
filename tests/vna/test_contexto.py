import datetime as dt
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
import requests

import pyield as yd
from pyield._internal.cache import ttl_cache  # noqa: PLC2701
from pyield.bc import sgs
from pyield.ipca import historico as ipca
from pyield.vna import _contexto, _download, _ntnb, _ntnc  # noqa: PLC2701

_DADOS = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def fontes_locais(monkeypatch):
    arquivos = {
        _ntnb._URL_ARQUIVO: "DP_Valores_Nominais_NTNB.xlsx",
        _ntnc._URL_ARQUIVO: "DP_Valores_Nominais_NTNC.xlsx",
    }
    monkeypatch.setattr(
        _download, "baixar_planilha", ttl_cache()(_download.baixar_planilha.__wrapped__)
    )
    monkeypatch.setattr(
        _download, "_buscar_conteudo", lambda url: (_DADOS / arquivos[url]).read_bytes()
    )
    monkeypatch.setattr(ipca, "_buscar_dados_api", lambda _: {})
    monkeypatch.setattr(sgs, "_chamar_api", lambda _: [])


def test_contexto_ipca_realizado_sem_vna_final(monkeypatch):
    competencias = []

    def buscar(url):
        competencias.append(url)
        return {"202610": "0.45"}

    monkeypatch.setattr(ipca, "_buscar_dados_api", buscar)
    resultado = yd.vna.contexto(
        "NTN-B", "22-10-2026", inflacao_projetada=Decimal("0.0050")
    )
    linha = resultado.row(0, named=True)

    assert linha["competencia"] == dt.date(2026, 10, 1)
    assert linha["vna_base"] == Decimal("4766.335112")
    assert linha["vna_fim"] is None
    assert linha["inflacao"] == Decimal("0.0045")
    assert linha["situacao_inflacao"] == "realizado"
    assert linha["origem_inflacao"] == "IBGE"
    assert linha["pendencias"] == []
    assert "/202610-202610/" in competencias[0]
    assert resultado.schema["vna_base"] == pl.Decimal(18, 6)


def test_contexto_igpm_seleciona_serie_e_taxa(monkeypatch):
    monkeypatch.setattr(
        sgs, "_chamar_api", lambda _: [{"data": "01/12/2025", "valor": "-0.01"}]
    )
    linha = yd.vna.contexto("NTN-C", "16/12/2025", "01/01/2031").row(0, named=True)

    assert linha["inicio_vigencia"] == dt.date(2025, 12, 1)
    assert linha["fim_vigencia"] == dt.date(2026, 1, 1)
    assert linha["vna_base"] == Decimal("6450.107485")
    assert linha["inflacao"] == Decimal("-0.0001")
    assert linha["origem_inflacao"] == "SGS/BCB"
    assert linha["situacao_inflacao"] == "realizado"


def test_projecao_explicita_e_pendencias():
    linha = yd.vna.contexto(
        "NTN-B", "22-10-2026", inflacao_projetada=Decimal("0.004567")
    ).row(0, named=True)
    assert linha["inflacao"] == Decimal("0.004567")
    assert linha["situacao_inflacao"] == "projetado"
    assert linha["origem_inflacao"] == "informada"
    assert linha["pendencias"] == []

    ausente = yd.vna.contexto("NTN-B", "22-10-2099").row(0, named=True)
    assert ausente["vna_base"] is None
    assert ausente["vna_fim"] is None
    assert ausente["inflacao"] is None
    assert ausente["origem_inflacao"] is None
    assert ausente["situacao_inflacao"] == "indisponivel"
    assert ausente["pendencias"] == ["vna_base_indisponivel", "inflacao_indisponivel"]


def test_projecao_nao_inventa_base():
    linha = yd.vna.contexto(
        "NTN-C", "22-10-2099", "01-01-2031", inflacao_projetada=0.0045
    ).row(0, named=True)
    assert linha["vna_base"] is None
    assert linha["situacao_inflacao"] == "projetado"
    assert linha["pendencias"] == ["vna_base_indisponivel"]


@pytest.mark.parametrize(
    ("titulo", "operacao", "liquidacao", "vencimento", "mudou"),
    [
        ("NTN-B", "14-08-2026", "17-08-2026", None, True),
        ("NTN-C", "30-09-2026", "01-10-2026", "01-01-2031", True),
        ("NTN-B", "01-10-2026", "02-10-2026", None, False),
    ],
)
def test_vigencias_independentes_na_operacao_e_liquidacao(
    titulo, operacao, liquidacao, vencimento, mudou
):
    d0 = yd.vna.contexto(titulo, operacao, vencimento).row(0, named=True)
    d1 = yd.vna.contexto(titulo, liquidacao, vencimento).row(0, named=True)
    assert (d0["inicio_vigencia"] != d1["inicio_vigencia"]) is mudou
    assert (d0["vna_base"] != d1["vna_base"]) is mudou


@pytest.mark.parametrize("projecao", [Decimal("NaN"), float("inf"), -1, -2])
def test_projecao_invalida(projecao):
    with pytest.raises(ValueError, match="projeção"):
        yd.vna.contexto("NTN-B", "22-10-2026", inflacao_projetada=projecao)


@pytest.mark.parametrize(
    ("titulo", "data", "vencimento"),
    [
        ("LFT", "22-10-2026", None),
        ("NTN-C", "22-10-2026", None),
        ("NTN-B", "22-10-2026", "01-01-2031"),
        ("NTN-B", "31-02-2026", None),
    ],
)
def test_entradas_invalidas(titulo, data, vencimento):
    with pytest.raises(ValueError):  # noqa: PT011 — valida entradas, sem fixar mensagem.
        yd.vna.contexto(titulo, data, vencimento)


def test_erro_de_fonte_nao_vira_parametro_indisponivel(monkeypatch):
    def falhar(_):
        raise requests.Timeout("IBGE indisponível")

    monkeypatch.setattr(ipca, "_buscar_dados_api", falhar)
    with pytest.raises(requests.Timeout):
        yd.vna.contexto("NTN-B", "22-10-2026")


def test_export_canonico():
    assert yd.vna.contexto is _contexto.contexto
    assert "contexto" in yd.vna.__all__
