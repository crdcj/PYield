"""Contratos de escala e precisão da precificação em base 100."""

from decimal import Decimal

import pytest

import pyield as yd


@pytest.mark.parametrize(
    "caso",
    [
        (
            yd.lft,
            "07-03-2014",
            "-0.000200009",
            "100.1158",
            "3451.215345",
            "3455.211852",
        ),
        (yd.ntnb, "15-08-2010", "0.082900009", "97.0813", "1728.461136", "1678.012540"),
        (yd.ntnc, "01-03-2011", "0.069000009", "99.0981", "2126.473734", "2107.295067"),
    ],
)
def test_exemplos_stn_em_base_100(caso):
    titulo, vencimento, taxa, cotacao, vna, pu = caso
    resultado = titulo.cotacao("21-05-2008", vencimento, Decimal(taxa))

    assert resultado == Decimal(cotacao)
    assert titulo.pu(Decimal(vna), resultado) == Decimal(pu)


@pytest.mark.parametrize("titulo", [yd.lft, yd.ntnb, yd.ntnc, yd.ntnbp, yd.ntnb1])
def test_pu_trunca_cotacao_percentual_antes_de_multiplicar(titulo):
    # T6 no VNA, T4 na cotação e T6 no PU, nessa ordem.
    assert titulo.pu(Decimal("1728.4611369"), Decimal("97.08139")) == Decimal(
        "1678.012540"
    )
    assert titulo.pu(Decimal("1728.4611369"), Decimal("100")) == Decimal("1728.461136")


@pytest.mark.parametrize(
    ("titulo", "vencimento", "cupom", "final"),
    [
        (yd.ntnb, "15-08-2010", 2.956301, 102.956301),
        (yd.ntnc, "01-03-2011", 2.956301, 102.956301),
        (yd.ntnc, "01-01-2031", 5.830052, 105.830052),
    ],
)
def test_fluxos_semestrais_em_base_100(titulo, vencimento, cupom, final):
    valores = titulo.fluxos_caixa("21-05-2008", vencimento)["valor_pagamento"]

    assert valores[:-1].to_list() == [cupom] * (len(valores) - 1)
    assert valores[-1] == final


@pytest.mark.parametrize("nome", list(yd.ntnb1.NomeComercial))
def test_amortizacoes_ntnb1_somam_100(nome):
    valores = yd.ntnb1.fluxos_caixa("10-05-2024", "15-12-2060", nome)["valor_pagamento"]

    assert valores.sum() == pytest.approx(100, abs=1e-12, rel=0)
    assert yd.ntnb1.cotacao("10-05-2024", "15-12-2060", 0, nome) == Decimal("100.0000")


@pytest.mark.parametrize("titulo", [yd.lft, yd.ntnbp])
def test_titulo_sem_cupom_com_taxa_zero_cota_100(titulo):
    assert titulo.cotacao("21-05-2008", "15-08-2010", 0) == Decimal("100.0000")
