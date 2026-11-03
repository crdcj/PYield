import datetime as dt
import json
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
import requests

import pyield as yd
from pyield.bc import sgs
from pyield.igpm import _indices as igpm  # noqa: PLC2701
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
        _download, "_buscar_conteudo", lambda url: (_DADOS / arquivos[url]).read_bytes()
    )
    monkeypatch.setattr(ipca, "_buscar_dados_api", lambda _: {})
    monkeypatch.setattr(sgs, "_chamar_api", lambda _: [])
    monkeypatch.setattr(
        igpm,
        "_buscar_historico",
        (_DADOS.parents[1] / "igpm/data/igpm_ipeadata_202609.json").read_bytes,
    )


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
    assert resultado.schema["inflacao"] == pl.Decimal(38, 4)
    assert linha["inflacao"].as_tuple() == Decimal("0.0045").as_tuple()


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


@pytest.mark.parametrize(
    "projecao", [0.004567, Decimal("0.004567"), "0.4567%", "0,4567%"]
)
def test_contexto_normaliza_sem_arredondar_e_alimenta_projecao(projecao):
    linha = yd.vna.contexto("NTN-B", "22-10-2026", inflacao_projetada=projecao).row(
        0, named=True
    )
    assert linha["inflacao"] == Decimal("0.004567")
    assert linha["situacao_inflacao"] == "projetado"
    assert yd.vna.projetado(
        linha["titulo"], linha["data"], linha["vna_base"], linha["inflacao"]
    ) == yd.vna.projetado("NTN-B", "22-10-2026", linha["vna_base"], "0.4567%")


@pytest.mark.parametrize("projecao", ["0.45", "0,45", "NaN%", "inf%", "abc", "-100%"])
def test_contexto_rejeita_percentual_invalido(projecao):
    with pytest.raises(ValueError, match="Taxa textual|projeção"):
        yd.vna.contexto("NTN-B", "22-10-2026", inflacao_projetada=projecao)


@pytest.mark.parametrize(
    ("projecao", "esperado", "escala"),
    [
        (0.0045, Decimal("0.0045"), 4),
        (Decimal("0.004500"), Decimal("0.004500"), 6),
        ("0,4500%", Decimal("0.004500"), 6),
        (
            Decimal("0.004567890123456789012345"),
            Decimal("0.004567890123456789012345"),
            24,
        ),
    ],
)
def test_contexto_preserva_escala_da_projecao(projecao, esperado, escala):
    resultado = yd.vna.contexto("NTN-B", "22-10-2026", inflacao_projetada=projecao)

    assert resultado.schema["inflacao"] == pl.Decimal(38, escala)
    assert resultado.item(0, "inflacao").as_tuple() == esperado.as_tuple()


def test_contexto_inflacao_indisponivel_usa_escala_das_taxas_oficiais():
    resultado = yd.vna.contexto("NTN-B", "22-10-2099")

    assert resultado.schema["inflacao"] == pl.Decimal(38, 4)
    assert resultado.item(0, "inflacao") is None


@pytest.fixture
def indices_ipca_locais(monkeypatch):
    dados = json.loads((_DADOS / "ipca_indices_200006_202609.json").read_bytes())
    serie = dados[0]["resultados"][0]["series"][0]["serie"]

    def buscar(url):
        if "/variaveis/2266?" not in url:
            return {}
        inicio, fim = url.split("/periodos/")[1].split("/")[0].split("-")
        return {
            periodo: valor
            for periodo, valor in serie.items()
            if inicio <= periodo <= fim
        }

    monkeypatch.setattr(ipca, "_buscar_dados_api", buscar)
    return serie


@pytest.mark.usefixtures("indices_ipca_locais")
@pytest.mark.parametrize(
    ("data", "anterior", "atual", "indice_anterior", "indice_atual"),
    [
        ("13-08-2026", "2026-06-01", "2026-07-01", "7652.37", "7657.73"),
        ("15-08-2026", "2026-07-01", "2026-08-01", "7657.73", "7633.23"),
        ("14-01-2026", "2025-11-01", "2025-12-01", "7378.94", "7403.29"),
        ("15-01-2026", "2025-12-01", "2026-01-01", "7403.29", "7427.72"),
    ],
)
def test_contexto_indices_ipca_seguem_vigencia(
    data, anterior, atual, indice_anterior, indice_atual
):
    resultado = yd.vna.contexto("NTN-B", data)
    linha = resultado.row(0, named=True)

    assert linha["competencia_anterior"] == dt.date.fromisoformat(anterior)
    assert linha["competencia"] == dt.date.fromisoformat(atual)
    assert linha["indice_anterior"] == Decimal(indice_anterior)
    assert linha["indice_atual"] == Decimal(indice_atual)
    assert linha["origem_indice"] == "IBGE"
    assert resultado.schema["indice_anterior"] == pl.Decimal(18, 2)
    assert resultado.schema["indice_atual"] == pl.Decimal(18, 2)
    assert linha["inflacao"] is None
    assert linha["situacao_inflacao"] == "indisponivel"


@pytest.mark.parametrize("periodos", [[], ["202606"], ["202607"], ["202606", "202607"]])
def test_contexto_disponibilidade_dos_indices_independe_da_taxa(
    monkeypatch, indices_ipca_locais, periodos
):
    def buscar(url):
        if "/variaveis/2266?" in url:
            assert "/202606-202607/" in url
            return {periodo: indices_ipca_locais[periodo] for periodo in periodos}
        return {"202607": "0.07"}

    monkeypatch.setattr(ipca, "_buscar_dados_api", buscar)
    resultado = yd.vna.contexto("NTN-B", "13-08-2026")
    linha = resultado.row(0, named=True)

    assert linha["indice_anterior"] == (
        Decimal("7652.37") if "202606" in periodos else None
    )
    assert linha["indice_atual"] == (
        Decimal("7657.73") if "202607" in periodos else None
    )
    assert linha["origem_indice"] == ("IBGE" if periodos else None)
    assert linha["inflacao"] == Decimal("0.0007")
    assert linha["situacao_inflacao"] == "realizado"
    assert linha["pendencias"] == []
    assert resultado.schema["indice_anterior"] == pl.Decimal(18, 2)
    assert resultado.schema["indice_atual"] == pl.Decimal(18, 2)


@pytest.mark.parametrize(
    ("data", "anterior", "atual", "indice_anterior", "indice_atual"),
    [
        (
            "30-09-2026",
            "2026-08-01",
            "2026-09-01",
            "1206.897",
            "1225.846",
        ),
        ("01-10-2026", "2026-09-01", "2026-10-01", "1225.846", None),
        ("01-08-1994", "1994-07-01", "1994-08-01", None, "100.000"),
    ],
)
def test_contexto_indices_igpm_preservam_escala_e_competencias(
    data, anterior, atual, indice_anterior, indice_atual
):
    resultado = yd.vna.contexto("NTN-C", data, "01-01-2031")
    linha = resultado.row(0, named=True)

    assert linha["competencia_anterior"] == dt.date.fromisoformat(anterior)
    assert linha["competencia"] == dt.date.fromisoformat(atual)
    if indice_anterior is None:
        assert linha["indice_anterior"] is None
    else:
        assert linha["indice_anterior"].as_tuple() == Decimal(indice_anterior).as_tuple()
    if indice_atual is None:
        assert linha["indice_atual"] is None
    else:
        assert linha["indice_atual"].as_tuple() == Decimal(indice_atual).as_tuple()
    assert linha["origem_indice"] == "FGV/Ipeadata"
    assert resultado.schema["indice_anterior"] == pl.Decimal(18, 3)
    assert resultado.schema["indice_atual"] == pl.Decimal(18, 3)
    assert linha["origem_inflacao"] is None


@pytest.mark.parametrize(
    ("titulo", "vencimento", "tipo_indice"),
    [("NTN-B", None, pl.Decimal(18, 2)), ("NTN-C", "01-01-2031", pl.Decimal(18, 3))],
)
def test_contexto_nao_estima_indices_com_projecao(titulo, vencimento, tipo_indice):
    resultado = yd.vna.contexto(
        titulo, "22-10-2099", vencimento, inflacao_projetada="0.45%"
    )
    linha = resultado.row(0, named=True)

    assert linha["indice_anterior"] is None
    assert linha["indice_atual"] is None
    assert linha["origem_indice"] is None
    assert resultado.schema["indice_anterior"] == tipo_indice
    assert resultado.schema["indice_atual"] == tipo_indice
    assert linha["situacao_inflacao"] == "projetado"
    assert linha["pendencias"] == ["vna_base_indisponivel"]


def test_contexto_ntnc_anterior_ao_corte_nao_expoe_indices():
    resultado = yd.vna.contexto("NTN-C", "01-07-1989", "01-01-2031")
    linha = resultado.row(0, named=True)

    assert linha["indice_anterior"] is None
    assert linha["indice_atual"] is None
    assert linha["origem_indice"] is None
    assert resultado.schema["indice_anterior"] == pl.Decimal(18, 3)
    assert resultado.schema["indice_atual"] == pl.Decimal(18, 3)


def test_contexto_indices_disponiveis_sem_vna_final(monkeypatch):
    def buscar(url):
        if "/variaveis/2266?" in url:
            # Simula divulgação de outubro antes do próximo VNA do Tesouro.
            return {"202609": "7695.82", "202610": "7730.45"}
        return {}

    monkeypatch.setattr(ipca, "_buscar_dados_api", buscar)
    linha = yd.vna.contexto("NTN-B", "22-10-2026").row(0, named=True)

    assert linha["vna_base"] == Decimal("4766.335112")
    assert linha["vna_fim"] is None
    assert linha["indice_anterior"] == Decimal("7695.82")
    assert linha["indice_atual"] == Decimal("7730.45")


def test_contexto_propaga_erro_na_fonte_dos_indices(monkeypatch):
    def falhar():
        raise requests.Timeout("Ipeadata indisponível")

    monkeypatch.setattr(igpm, "_buscar_historico", falhar)
    with pytest.raises(requests.Timeout, match="Ipeadata"):
        yd.vna.contexto("NTN-C", "30-09-2026", "01-01-2031")
