import math
from decimal import Decimal

import polars as pl
import pytest

from pyield import du, ntnc


def test_cotacao_e_pu_reproduzem_valores_de_referencia() -> None:
    cotacao = ntnc.cotacao("21-03-2025", "01-01-2031", 0.067626)
    pu = ntnc.pu(6598.913723, cotacao)

    assert cotacao == Decimal("126.4958")
    assert pu == Decimal("8347.348705")


def test_cotacao_e_pu_nulos_retornam_decimal_nan() -> None:
    assert ntnc.cotacao(None, "01-01-2031", 0.067626).is_nan()
    assert ntnc.pu(float("nan"), 126.4958).is_nan()


def test_taxa_aceita_pu_decimal() -> None:
    taxa_esperada = Decimal("0.06762593")
    pu = ntnc.pu(6598.913723, ntnc.cotacao("21-03-2025", "01-01-2031", 0.067626))

    assert ntnc.taxa("21-03-2025", "01-01-2031", 6598.913723, pu) == taxa_esperada


def test_cotacao_trunca_taxa_percentual_excedente() -> None:
    assert ntnc.cotacao("21-05-2008", "01-03-2011", "6.9000009%") == Decimal("99.0981")


def test_pu_trunca_vna_e_cotacao_excedentes() -> None:
    assert ntnc.pu(2126.4737349, 99.09819) == Decimal("2107.295067")


@pytest.mark.parametrize("vencimento", ["01-01-2031", "01-01-2032"])
@pytest.mark.parametrize("taxa", [-0.02, 0.0, 0.067626])
def test_curva_plana_reproduz_desconto_dos_fluxos(vencimento, taxa):
    liquidacao = "21-03-2025"
    curva = pl.DataFrame({"dias_uteis": [252], "taxa_zero": [taxa]})
    fluxos = ntnc.fluxos_caixa(liquidacao, vencimento)
    prazos = du.contar(liquidacao, fluxos["data_pagamento"])
    esperado = sum(
        round(valor / (1 + taxa) ** (math.trunc(dias / 252 * 1e14) / 1e14), 10)
        for valor, dias in zip(fluxos["valor_pagamento"], prazos, strict=True)
    )

    assert ntnc.cotacao_curva_zero(liquidacao, vencimento, curva) == pytest.approx(
        esperado, abs=1e-10, rel=0
    )
    assert ntnc.taxa_curva_zero(liquidacao, vencimento, curva) == taxa


def test_curva_inclinada_reproduz_cotacao_alvo():
    liquidacao, vencimento = "21-03-2025", "01-01-2031"
    prazo_curto, prazo_longo = 252, 1260
    taxa_curta, taxa_longa = 0.04, 0.08
    curva = pl.DataFrame(
        {
            "dias_uteis": [prazo_curto, prazo_longo],
            "taxa_zero": [taxa_curta, taxa_longa],
        }
    )
    fluxos = ntnc.fluxos_caixa(liquidacao, vencimento)
    prazos = du.contar(liquidacao, fluxos["data_pagamento"])

    def taxa_no_prazo(dias):
        if dias <= prazo_curto:
            return 0.04
        if dias >= prazo_longo:
            return 0.08
        fator = 1.04 * (1.08**5 / 1.04) ** ((dias - 252) / 1008)
        return fator ** (252 / dias) - 1

    esperado = sum(
        round(valor / (1 + taxa_no_prazo(dias)) ** (dias / 252), 10)
        for valor, dias in zip(fluxos["valor_pagamento"], prazos, strict=True)
    )
    cotacao = ntnc.cotacao_curva_zero(liquidacao, vencimento, curva)
    taxa = ntnc.taxa_curva_zero(liquidacao, vencimento, curva)
    equivalente = sum(
        round(valor / (1 + taxa) ** (dias / 252), 10)
        for valor, dias in zip(fluxos["valor_pagamento"], prazos, strict=True)
    )

    assert cotacao == pytest.approx(esperado, abs=1e-9, rel=0)
    assert equivalente == pytest.approx(cotacao, abs=1e-8, rel=0)
    assert taxa_curta < taxa < taxa_longa
    assert taxa != round(taxa, 4)
    assert ntnc.cotacao_curva_zero(liquidacao, vencimento, curva.reverse()) == cotacao


@pytest.mark.parametrize("funcao", [ntnc.cotacao_curva_zero, ntnc.taxa_curva_zero])
@pytest.mark.parametrize(
    "curva",
    [
        pl.DataFrame(),
        pl.DataFrame(schema={"dias_uteis": pl.Int64, "taxa_zero": pl.Float64}),
        pl.DataFrame({"dias_uteis": [252, 252], "taxa_zero": [0.05, 0.06]}),
        *[
            pl.DataFrame({"dias_uteis": [dias], "taxa_zero": [taxa]})
            for dias, taxa in [
                (0, 0.05),
                (-1, 0.05),
                (1.5, 0.05),
                (None, 0.05),
                (252, None),
                (252, -1.0),
                (252, float("nan")),
                (252, float("inf")),
                (float("inf"), 0.05),
            ]
        ],
    ],
)
def test_curva_zero_rejeita_vertices_invalidos(funcao, curva):
    with pytest.raises(ValueError, match="Curva"):
        funcao("21-03-2025", "01-01-2031", curva)


@pytest.mark.parametrize("funcao", [ntnc.cotacao_curva_zero, ntnc.taxa_curva_zero])
@pytest.mark.parametrize("liquidacao", [None, "01-01-2031", "02-01-2031"])
def test_curva_zero_sem_fluxos_retorna_nan(funcao, liquidacao):
    curva = pl.DataFrame({"dias_uteis": [252], "taxa_zero": [0.05]})
    assert math.isnan(funcao(liquidacao, "01-01-2031", curva))


def test_curva_zero_resultado_nao_finito_retorna_nan():
    curva = pl.DataFrame({"dias_uteis": [252], "taxa_zero": [-0.9999999999999999]})
    assert math.isnan(ntnc.cotacao_curva_zero("21-03-2025", "01-01-2100", curva))
    assert math.isnan(ntnc.taxa_curva_zero("21-03-2025", "01-01-2100", curva))


@pytest.mark.parametrize("liquidacao", ["21-03-2025", "31-12-2030"])
def test_taxa_curva_zero_aceita_taxas_acima_do_limite_da_busca_automatica(liquidacao):
    taxa = 12.0
    curva = pl.DataFrame({"dias_uteis": [252], "taxa_zero": [taxa]})

    assert ntnc.taxa_curva_zero(liquidacao, "01-01-2031", curva) == taxa


def test_taxa_curva_zero_propaga_falha_de_convergencia(monkeypatch):
    def sem_convergencia(*args, **kwargs):
        return float("nan")

    monkeypatch.setattr("pyield.tpf.titulos._utils.encontrar_raiz", sem_convergencia)
    curva = pl.DataFrame({"dias_uteis": [252, 1260], "taxa_zero": [0.04, 0.08]})

    assert math.isnan(ntnc.taxa_curva_zero("21-03-2025", "01-01-2031", curva))


def test_curva_zero_reproduz_cotacao_da_planilha_sistax():
    """Confere os valores salvos da aba Curva Zero da Sistax v4.6.1(3).

    C1: liquidação; AZ5: vencimento; BA5:BB13: prazos e taxas por fluxo;
    BD5:BD13: pagamentos; soma de BC5:BC13: cotação sem truncamento.
    Os vértices abaixo estão em ordem crescente, inversa à da planilha.
    A TIR salva em BB3 não é referência de igualdade: a planilha compara
    cotações truncadas a quatro casas, enquanto a API resolve o alvo bruto.
    """
    liquidacao, vencimento = "21-09-2026", "01-01-2031"
    prazos = [70, 193, 321, 445, 569, 693, 818, 941, 1070]
    curva = pl.DataFrame(
        {
            "dias_uteis": prazos,
            "taxa_zero": [
                0.05900000071632405,
                0.0627064967323272,
                0.07018174860801629,
                0.07333817548195753,
                0.07403087579909262,
                0.07436933136583579,
                0.07505968370954208,
                0.07556023671967105,
                0.07547339241495621,
            ],
        }
    )
    cotacao_esperada = 118.2970250079
    pagamentos = [5.830052] * 8 + [105.830052]

    fluxos = ntnc.fluxos_caixa(liquidacao, vencimento)
    assert du.contar(liquidacao, fluxos["data_pagamento"]).to_list() == prazos
    assert fluxos["valor_pagamento"].to_list() == pagamentos
    assert ntnc.cotacao_curva_zero(liquidacao, vencimento, curva) == cotacao_esperada

    taxa = ntnc.taxa_curva_zero(liquidacao, vencimento, curva)
    cotacao_equivalente = sum(
        round(valor / (1 + taxa) ** (math.trunc(dias / 252 * 1e14) / 1e14), 10)
        for valor, dias in zip(pagamentos, prazos, strict=True)
    )
    assert isinstance(taxa, float)
    assert cotacao_equivalente == pytest.approx(cotacao_esperada, abs=1e-8, rel=0)
