import datetime as dt

import polars as pl
import pytest

import pyield as yd
from pyield import ntnb1, ntnbp

ntnb_td = ntnbp

DATA_LIQUIDACAO = dt.date(2026, 7, 13)
VENCIMENTOS = [
    dt.date(2026, 8, 15),
    dt.date(2027, 5, 15),
    dt.date(2028, 8, 15),
    dt.date(2029, 5, 15),
    dt.date(2030, 8, 15),
    dt.date(2031, 5, 15),
    dt.date(2032, 8, 15),
    dt.date(2033, 5, 15),
    dt.date(2035, 5, 15),
    dt.date(2037, 5, 15),
    dt.date(2040, 8, 15),
    dt.date(2045, 5, 15),
    dt.date(2050, 8, 15),
    dt.date(2055, 5, 15),
    dt.date(2060, 8, 15),
]
TAXAS_TIR = [
    0.1167,
    0.0844,
    0.0853,
    0.0832,
    0.0832,
    0.0822,
    0.0816,
    0.0809,
    0.0799,
    0.0787,
    0.0771,
    0.0753,
    0.0748,
    0.0741,
    0.0740,
]
TAXAS_ZERO_PLANILHA = [
    0.11669999999923197,
    0.08432556565343718,
    0.0852576487182215,
    0.08306374259814908,
    0.0830739926717361,
    0.08198625118890712,
    0.08130205887845499,
    0.08050949086848868,
    0.07934027697049251,
    0.07782879514830321,
    0.07568300268997708,
    0.07307847693229963,
    0.07245435891438645,
    0.07110155760681147,
    0.0710829915123008,
]
FORWARDS_PLANILHA = [
    0.11669999999922916,
    0.08002323895627329,
    0.08587948250549778,
    0.07682830620923183,
    0.08309713730298153,
    0.0760231708121404,
    0.07870478409822645,
    0.07405926064065749,
    0.075333807599243515,
    0.071177613235201516,
    0.06859414142571751,
    0.06536577256663077,
    0.07021764042199136,
    0.064262548851535373,
    0.070981172708711196,
]


def test_namespace_dos_titulos_separado_do_ntnb_anbima():
    assert not hasattr(yd.ntnb, "taxas_zero_td")
    assert yd.ntnb1 is ntnb1
    assert yd.ntnbp is ntnbp


@pytest.mark.parametrize(
    ("data_liquidacao", "esperado"),
    [
        (
            dt.date(2026, 7, 13),
            [dt.date(2026, 7, 15), dt.date(2026, 8, 15), dt.date(2026, 9, 15)],
        ),
        (
            dt.date(2026, 7, 15),
            [dt.date(2026, 7, 15), dt.date(2026, 8, 15), dt.date(2026, 9, 15)],
        ),
        (
            dt.date(2026, 7, 16),
            [dt.date(2026, 8, 15), dt.date(2026, 9, 15)],
        ),
    ],
)
def test_gerar_vertices_mensais(data_liquidacao, esperado):
    resultado = ntnb_td.taxas_zero(
        data_liquidacao,
        [dt.date(2026, 9, 15)],
        [0.1],
        incluir_vertices=True,
    )
    assert resultado["data_vencimento"].to_list() == esperado


def test_taxas_zero_td_reproduz_planilha_curva_zero():
    """A calibração deve reproduzir os vértices da aba Curva Zero."""
    resultado = ntnb_td.taxas_zero(DATA_LIQUIDACAO, VENCIMENTOS, TAXAS_TIR)

    esperado = pl.DataFrame(
        {
            "data_vencimento": VENCIMENTOS,
            "taxa_zero": TAXAS_ZERO_PLANILHA,
            "taxa_forward": FORWARDS_PLANILHA,
        }
    )

    assert resultado["data_vencimento"].to_list() == VENCIMENTOS
    assert resultado["taxa_zero"].to_list() == pytest.approx(
        esperado["taxa_zero"].to_list(), abs=1e-8
    )
    assert resultado["taxa_forward"].to_list() == pytest.approx(
        esperado["taxa_forward"].to_list(), abs=1e-8
    )


@pytest.mark.parametrize(
    ("vencimento", "taxa_mercado", "taxa_compra", "taxa_venda"),
    [
        (dt.date(2026, 8, 15), 0.1167, 0.1163, 0.1175),
        (dt.date(2029, 5, 15), 0.0831, 0.0827, 0.0839),
        (dt.date(2032, 8, 15), 0.0813, 0.0809, 0.0821),
        (dt.date(2035, 5, 15), 0.0793, 0.0789, 0.0801),
        (dt.date(2040, 8, 15), 0.0757, 0.0753, 0.0765),
        (dt.date(2045, 5, 15), 0.0731, 0.0727, 0.0739),
        (dt.date(2050, 8, 15), 0.0725, 0.0721, 0.0733),
    ],
)
def test_taxas_zero_td_reproduz_taxas_ntnb_principal(
    vencimento,
    taxa_mercado,
    taxa_compra,
    taxa_venda,
):
    curva = ntnb_td.taxas_zero(
        DATA_LIQUIDACAO,
        VENCIMENTOS,
        TAXAS_TIR,
        incluir_vertices=True,
    )
    taxa_zero = ntnbp.taxa(DATA_LIQUIDACAO, vencimento, curva)

    assert round(taxa_zero, 4) == taxa_mercado
    assert round(taxa_mercado - 0.0004, 4) == taxa_compra
    assert round(taxa_mercado + 0.0008, 4) == taxa_venda


def test_ntnb1_cotacao_curva_zero_reproduz_planilha_td():
    curva_zero = ntnb_td.taxas_zero(
        DATA_LIQUIDACAO,
        VENCIMENTOS,
        TAXAS_TIR,
        incluir_vertices=True,
    )
    casos = [
        (
            dt.date(2030, 12, 15),
            ntnb1.NomeComercial.EDUCA_MAIS,
            0.7578968107729999,
            0.08381729701801194,
        ),
        (
            dt.date(2048, 12, 15),
            ntnb1.NomeComercial.EDUCA_MAIS,
            0.24830136813400006,
            0.07298838017384301,
        ),
        (
            dt.date(2049, 12, 15),
            ntnb1.NomeComercial.RENDA_MAIS,
            0.4080115710080001,
            0.0762715580535314,
        ),
        (
            dt.date(2084, 12, 15),
            ntnb1.NomeComercial.RENDA_MAIS,
            0.03949286761799999,
            0.0710829913301495,
        ),
    ]

    for vencimento, nome_comercial, cotacao_esperada, taxa_esperada in casos:
        cotacao = ntnb1.cotacao_curva_zero(
            DATA_LIQUIDACAO,
            vencimento,
            curva_zero,
            nome_comercial,
        )
        taxa = ntnb1.taxa_curva_zero(
            DATA_LIQUIDACAO,
            vencimento,
            curva_zero,
            nome_comercial,
        )
        assert cotacao == pytest.approx(cotacao_esperada, abs=2e-9)
        assert taxa == pytest.approx(taxa_esperada, abs=1e-12)
