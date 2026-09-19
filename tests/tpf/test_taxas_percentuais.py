"""Entrada percentual explícita nos cálculos públicos de títulos."""

from decimal import Decimal

import polars as pl
import pytest

import pyield as yd

LIQUIDACAO = "31-05-2024"
VENCIMENTO = "15-05-2035"
RENDA = yd.ntnb1.NomeComercial.RENDA_MAIS


@pytest.mark.parametrize(
    ("funcao", "extras"),
    [
        (yd.ltn.pu, ()),
        (yd.ntnf.pu, ()),
        (yd.lft.cotacao, ()),
        (yd.ntnb.cotacao, ()),
        (yd.ntnc.cotacao, ()),
        (yd.ntnbp.cotacao, ()),
        (yd.ntnb1.cotacao, (RENDA,)),
    ],
)
@pytest.mark.parametrize(
    ("texto", "decimal"),
    [
        ("5.75%", "0.0575"),
        ("5,75%", "0.0575"),
        (" +5,75 % ", "0.0575"),
        ("-0.02%", "-0.0002"),
        ("0%", "0"),
        ("5.7500009%", "0.057500009"),
        ("575%", "5.75"),
    ],
)
def test_precificacao_percentual_equivale_a_decimal(funcao, extras, texto, decimal):
    esperado = funcao(LIQUIDACAO, VENCIMENTO, Decimal(decimal), *extras)
    assert funcao(LIQUIDACAO, VENCIMENTO, texto, *extras) == esperado


@pytest.mark.parametrize(
    "texto", ["5.75", "5,75", "", " ", "%", "NaN%", "Inf%", "1.000,5%", "5%%", "1e2%"]
)
@pytest.mark.parametrize("funcao", [yd.ltn.pu, yd.ntnb.cotacao, yd.ntnbp.cotacao])
def test_percentual_malformado_e_rejeitado(funcao, texto):
    with pytest.raises(ValueError, match="percentual"):
        funcao(LIQUIDACAO, VENCIMENTO, texto)


@pytest.mark.parametrize("titulo", [yd.ntnb, yd.ntnc, yd.ntnf, yd.ntnb1])
def test_duration_aceita_percentual_sem_truncar_taxa(titulo):
    extras = (RENDA,) if titulo is yd.ntnb1 else ()
    assert titulo.duration(LIQUIDACAO, VENCIMENTO, "5.7500009%", *extras) == (
        titulo.duration(LIQUIDACAO, VENCIMENTO, 0.057500009, *extras)
    )


@pytest.mark.parametrize(
    "titulo", [yd.ltn, yd.ntnb, yd.ntnc, yd.ntnf, yd.ntnbp, yd.ntnb1]
)
def test_dv01_aceita_percentual(titulo):
    extras = (RENDA,) if titulo is yd.ntnb1 else ()
    assert titulo.dv01(LIQUIDACAO, VENCIMENTO, "5,75%", 1000, *extras) == (
        titulo.dv01(LIQUIDACAO, VENCIMENTO, 0.0575, 1000, *extras)
    )


@pytest.mark.parametrize("titulo", [yd.ltn, yd.lft])
def test_rentabilidade_aceita_ambas_as_taxas_percentuais(titulo):
    assert titulo.rentabilidade("5,75%", "10%") == titulo.rentabilidade(0.0575, 0.10)


@pytest.mark.parametrize("funcao", [yd.ntnf.rentabilidade, yd.ntnf.premio_limpo])
def test_analise_ntnf_aceita_taxa_percentual(funcao):
    argumentos = {
        "data_liquidacao": "23-08-2024",
        "data_vencimento": "01-01-2035",
        "vencimentos_di": ["01-01-2025", "01-01-2027", "01-01-2035"],
        "taxas_di": [0.10823, 0.11594, 0.11531],
    }
    assert funcao(taxa_ntnf="11.6586%", **argumentos) == funcao(
        taxa_ntnf=0.116586, **argumentos
    )


def test_expr_continua_interpretando_string_como_nome_de_coluna():
    df = pl.DataFrame({"5.75%": [0.0575], "di": [0.1]})
    assert df.select(yd.ltn.rentabilidade_expr("5.75%", "di")).item() == (
        yd.ltn.rentabilidade(0.0575, 0.1)
    )
