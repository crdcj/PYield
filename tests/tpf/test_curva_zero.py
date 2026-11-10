"""Prazos repetidos na curva zero de NTN-B Principal, NTN-B1 e NTN-C."""

import math
from functools import partial

import polars as pl
import pytest

import pyield as yd
from pyield import ntnb1, ntnbp, ntnc

RENDA_MAIS = ntnb1.NomeComercial.RENDA_MAIS
LIQUIDACAO = "03-01-2025"

FUNCOES = [
    partial(ntnbp.taxa, LIQUIDACAO, "15-05-2028"),
    partial(
        ntnb1.cotacao_curva_zero, LIQUIDACAO, "15-12-2050", nome_comercial=RENDA_MAIS
    ),
    partial(ntnb1.taxa_curva_zero, LIQUIDACAO, "15-12-2050", nome_comercial=RENDA_MAIS),
    partial(ntnc.cotacao_curva_zero, LIQUIDACAO, "01-01-2031"),
    partial(ntnc.taxa_curva_zero, LIQUIDACAO, "01-01-2031"),
]
IDS = [
    "ntnbp.taxa",
    "ntnb1.cotacao_curva_zero",
    "ntnb1.taxa_curva_zero",
    "ntnc.cotacao_curva_zero",
    "ntnc.taxa_curva_zero",
]

CURVA = pl.DataFrame({"dias_uteis": [500, 2000], "taxa_zero": [0.07, 0.075]})


@pytest.mark.parametrize("funcao", FUNCOES, ids=IDS)
def test_prazo_repetido_com_taxas_diferentes_gera_erro(funcao):
    curva = pl.DataFrame(
        {"dias_uteis": [500, 500, 2000], "taxa_zero": [0.07, 0.08, 0.075]}
    )
    with pytest.raises(ValueError, match="mesmo prazo com taxas diferentes; .*DU 500"):
        funcao(curva_zero=curva)


@pytest.mark.parametrize("funcao", FUNCOES, ids=IDS)
def test_vertice_repetido_identico_equivale_a_curva_sem_repeticao(funcao):
    curva_repetida = pl.concat([CURVA, CURVA.head(1)])
    assert funcao(curva_zero=curva_repetida) == funcao(curva_zero=CURVA)


def _vertices(titulo: str) -> tuple[pl.Series, pl.Series]:
    df = yd.tpf.taxas("23-08-2024", titulo)
    return df["data_vencimento"], df["taxa_indicativa"]


def _repetir_terceiro(
    vencimentos: pl.Series, taxas: pl.Series, delta: float
) -> tuple[pl.Series, pl.Series]:
    return (
        pl.concat([vencimentos, vencimentos[2:3]]),
        pl.concat([taxas, taxas[2:3] + delta]),
    )


def _bootstraps():
    ltn = _vertices("LTN")
    ntnf_ = _vertices("NTN-F")
    ntnb_ = _vertices("NTN-B")
    data = "23-08-2024"
    return [
        (
            "ntnf.taxas_zero[LTN]",
            lambda v, t: yd.ntnf.taxas_zero(data, v, t, *ntnf_),
            ltn,
        ),
        (
            "ntnf.taxas_zero[NTN-F]",
            lambda v, t: yd.ntnf.taxas_zero(data, *ltn, v, t),
            ntnf_,
        ),
        (
            "ntnf.taxas_zero_forwards[LTN]",
            lambda v, t: yd.ntnf.taxas_zero_forwards(data, v, t, *ntnf_),
            ltn,
        ),
        ("ntnb.taxas_zero", lambda v, t: yd.ntnb.taxas_zero(data, v, t), ntnb_),
    ]


@pytest.mark.rede
def test_vencimento_repetido_nos_bootstraps():
    """Repetição idêntica é ignorada; taxas diferentes no mesmo vencimento, erro."""
    for nome, funcao, (vencimentos, taxas) in _bootstraps():
        esperado = funcao(vencimentos, taxas)
        assert funcao(*_repetir_terceiro(vencimentos, taxas, 0.0)).equals(esperado), (
            nome
        )
        with pytest.raises(ValueError, match="duplicados .* com taxas diferentes"):
            funcao(*_repetir_terceiro(vencimentos, taxas, 0.01))


FUNCOES_CURVA_NTNB1 = [ntnb1.cotacao_curva_zero, ntnb1.taxa_curva_zero]


@pytest.mark.parametrize("funcao", FUNCOES_CURVA_NTNB1)
def test_ntnb1_sem_pagamentos_apos_liquidacao_retorna_nan(funcao):
    """A última amortização ocorre no dia 15; depois dela não há fluxos."""
    curva = pl.DataFrame({"dias_uteis": [100, 5000], "taxa_zero": [0.06, 0.065]})

    assert math.isnan(funcao("17-12-2050", "20-12-2050", curva, RENDA_MAIS))


@pytest.mark.parametrize("funcao", FUNCOES_CURVA_NTNB1)
def test_ntnb1_curva_sem_vertice_valido_gera_erro(funcao):
    curva = pl.DataFrame({"dias_uteis": [None], "taxa_zero": [0.06]})

    with pytest.raises(ValueError, match="ao menos um vértice válido"):
        funcao(LIQUIDACAO, "15-12-2084", curva, RENDA_MAIS)


@pytest.mark.parametrize("taxa", [float("inf"), -1.0])
def test_taxas_zero_ntnb_rejeita_taxa_fora_do_dominio(taxa):
    with pytest.raises(ValueError, match="finitas e maiores que -1"):
        yd.ntnb.taxas_zero("05-09-2024", ["15-05-2025", "15-08-2026"], [0.06, taxa])


@pytest.mark.parametrize("taxa_ausente", [float("nan"), None])
def test_taxas_zero_ignoram_vertice_sem_taxa(taxa_ausente):
    """Taxa nula ou NaN é dado ausente: o vértice sai do cálculo e do resultado."""
    vencimentos = ["15-05-2025", "15-08-2026", "15-05-2027"]
    com_ausente = yd.ntnb.taxas_zero(
        "05-09-2024", vencimentos, [0.06, taxa_ausente, 0.062]
    )
    sem_o_vertice = yd.ntnb.taxas_zero(
        "05-09-2024", [vencimentos[0], vencimentos[2]], [0.06, 0.062]
    )

    assert com_ausente.equals(sem_o_vertice)


@pytest.mark.parametrize("taxa_ausente", [float("nan"), None])
def test_taxas_zero_ntnf_ignoram_vertice_sem_taxa(taxa_ausente):
    ltn = (["2025-01-01"], [0.108])
    vencimentos = ["2027-01-01", "2029-01-01", "2031-01-01"]
    com_ausente = yd.ntnf.taxas_zero(
        "03-09-2024", *ltn, vencimentos, [0.119, 0.12, taxa_ausente]
    )
    sem_o_vertice = yd.ntnf.taxas_zero(
        "03-09-2024", *ltn, vencimentos[:2], [0.119, 0.12]
    )

    assert com_ausente.equals(sem_o_vertice)


@pytest.mark.parametrize("liquidacao", ["15-12-2084", "16-12-2084", "02-01-2090"])
def test_ntnb1_vencida_nao_tem_fluxos(liquidacao):
    """Título vencido ou vencendo na liquidação não tem fluxo, e isso não é erro."""
    vencimento = "15-12-2084"
    curva = pl.DataFrame({"dias_uteis": [100, 5000], "taxa_zero": [0.06, 0.065]})

    assert ntnb1.datas_pagamento(liquidacao, vencimento, RENDA_MAIS).is_empty()
    assert ntnb1.fluxos_caixa(liquidacao, vencimento, RENDA_MAIS).is_empty()
    assert ntnb1.cotacao(liquidacao, vencimento, 0.06, RENDA_MAIS).is_nan()
    assert math.isnan(ntnb1.duration(liquidacao, vencimento, 0.06, RENDA_MAIS))
    assert math.isnan(ntnb1.dv01(liquidacao, vencimento, 0.06, 100.0, RENDA_MAIS))
    for funcao in FUNCOES_CURVA_NTNB1:
        assert math.isnan(funcao(liquidacao, vencimento, curva, RENDA_MAIS))
