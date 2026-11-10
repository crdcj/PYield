"""Prazos repetidos na curva zero de NTN-B Principal, NTN-B1 e NTN-C."""

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
