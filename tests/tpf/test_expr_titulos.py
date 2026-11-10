import math
from decimal import Decimal

import polars as pl
import pytest

from pyield import du, lft, ltn, ntnb, ntnb1, ntnbp, ntnc, ntnf


@pytest.mark.parametrize("nome", ["rentabilidade", "premio_limpo"])
def test_ntnf_expr_preserva_linhas_apos_falha_do_solver(monkeypatch, nome):
    escalar = getattr(ntnf, nome)
    expressao = getattr(ntnf, f"{nome}_expr")
    parametros = {
        "data_liquidacao": "26-03-2025",
        "vencimentos_di": ["01-01-2030", "01-01-2035"],
        "taxas_di": [0.11594, 0.11531],
    }
    esperado = escalar(**parametros, data_vencimento="01-01-2035", taxa_ntnf=0.151375)
    resolver = ntnf._utils.encontrar_raiz
    chamadas = 0

    def resolver_com_falha(func, intervalo=None):
        nonlocal chamadas
        chamadas += 1
        if chamadas == 2:  # noqa: PLR2004
            return float("nan")
        return resolver(func, intervalo=intervalo)

    monkeypatch.setattr(ntnf._utils, "encontrar_raiz", resolver_com_falha)
    resultado = pl.DataFrame(
        {"vencimento": ["01-01-2035"] * 3, "taxa": [0.151375] * 3}
    ).select(
        resultado=expressao(
            **parametros, data_vencimento="vencimento", taxa_ntnf="taxa"
        )
    )["resultado"]

    assert resultado.dtype == pl.Float64
    assert resultado[0] == pytest.approx(esperado)
    assert math.isnan(resultado[1])
    assert resultado[2] == pytest.approx(esperado)


@pytest.mark.parametrize("nome", ["rentabilidade_expr", "premio_limpo_expr"])
def test_ntnf_expr_propaga_erro_de_entrada(nome):
    with pytest.raises(ValueError, match="Data inválida"):
        pl.DataFrame({"vencimento": ["data inválida"], "taxa": [0.15]}).select(
            getattr(ntnf, nome)(
                data_liquidacao="26-03-2025",
                data_vencimento="vencimento",
                taxa_ntnf="taxa",
                vencimentos_di=["01-01-2030", "01-01-2035"],
                taxas_di=[0.11594, 0.11531],
            )
        )


def test_lft_rentabilidade_expr_bate_com_calculo_escalar():
    taxa_lft = 0.001124
    taxa_di = 0.13967670224373396

    resultado = pl.DataFrame(
        {
            "taxa_lft": [taxa_lft],
            "taxa_di": [taxa_di],
        }
    ).select(
        rentabilidade=lft.rentabilidade_expr("taxa_lft", "taxa_di"),
    )

    assert resultado["rentabilidade"][0] == pytest.approx(
        lft.rentabilidade(taxa_lft, taxa_di)
    )


def test_ltn_exprs_batem_com_calculos_escalares():
    data_liquidacao = "26-03-2025"
    data_vencimento = "01-01-2032"
    taxa = 0.150970

    resultado = pl.DataFrame(
        {
            "data_liquidacao": [data_liquidacao],
            "data_vencimento": [data_vencimento],
            "taxa": [taxa],
            "taxa_di": [0.149],
        }
    ).select(
        duration=ltn.duration_expr("data_liquidacao", "data_vencimento"),
        rentabilidade=ltn.rentabilidade_expr("taxa", "taxa_di"),
        dv01=ltn.dv01_expr("data_liquidacao", "data_vencimento", "taxa"),
    )

    assert resultado["duration"][0] == pytest.approx(
        du.contar(data_liquidacao, data_vencimento) / 252
    )
    assert resultado["dv01"][0] == pytest.approx(
        ltn.dv01(data_liquidacao, data_vencimento, taxa)
    )
    assert resultado["rentabilidade"][0] == pytest.approx(
        ltn.rentabilidade(taxa, 0.149)
    )


def test_ntnf_exprs_batem_com_calculos_escalares():
    data_liquidacao = "26-03-2025"
    data_vencimento = "01-01-2035"
    taxa = 0.151375
    pu = ntnf.pu(data_liquidacao, data_vencimento, taxa)

    resultado = pl.DataFrame(
        {
            "data_liquidacao": [data_liquidacao],
            "data_vencimento": [data_vencimento],
            "taxa": [taxa],
            "pu": [pu],
        }
    ).select(
        duration=ntnf.duration_expr("data_liquidacao", "data_vencimento", "taxa"),
        rentabilidade=ntnf.rentabilidade_expr(
            data_liquidacao=data_liquidacao,
            data_vencimento="data_vencimento",
            taxa_ntnf="taxa",
            vencimentos_di=[
                "2025-01-01",
                "2030-01-01",
                "2035-01-01",
            ],
            taxas_di=[0.10823, 0.11594, 0.11531],
        ),
        premio_limpo=ntnf.premio_limpo_expr(
            data_liquidacao=data_liquidacao,
            data_vencimento="data_vencimento",
            taxa_ntnf="taxa",
            vencimentos_di=[
                "2025-01-01",
                "2030-01-01",
                "2035-01-01",
            ],
            taxas_di=[0.10823, 0.11594, 0.11531],
        ),
        dv01=ntnf.dv01_expr("data_liquidacao", "data_vencimento", "taxa"),
    )

    assert resultado["duration"][0] == pytest.approx(
        ntnf.duration(data_liquidacao, data_vencimento, taxa)
    )
    assert resultado["dv01"][0] == pytest.approx(
        ntnf.dv01(data_liquidacao, data_vencimento, taxa)
    )
    assert resultado["rentabilidade"][0] == pytest.approx(
        ntnf.rentabilidade(
            data_liquidacao=data_liquidacao,
            data_vencimento=data_vencimento,
            taxa_ntnf=taxa,
            vencimentos_di=["2025-01-01", "2030-01-01", "2035-01-01"],
            taxas_di=[0.10823, 0.11594, 0.11531],
        )
    )
    assert resultado["premio_limpo"][0] == pytest.approx(
        ntnf.premio_limpo(
            data_liquidacao=data_liquidacao,
            data_vencimento=data_vencimento,
            taxa_ntnf=taxa,
            vencimentos_di=["2025-01-01", "2030-01-01", "2035-01-01"],
            taxas_di=[0.10823, 0.11594, 0.11531],
        )
    )


def test_ntnb_exprs_batem_com_calculos_escalares():
    data_liquidacao = "26-03-2025"
    data_vencimento = "15-08-2060"
    taxa = 0.074358
    vna = 4470.979474
    pu = ntnb.pu(vna, ntnb.cotacao(data_liquidacao, data_vencimento, taxa))

    resultado = pl.DataFrame(
        {
            "data_liquidacao": [data_liquidacao],
            "data_vencimento": [data_vencimento],
            "taxa": [taxa],
            "pu": [pu],
        }
    ).select(
        duration=ntnb.duration_expr("data_liquidacao", "data_vencimento", "taxa"),
        dv01=ntnb.dv01_expr("data_liquidacao", "data_vencimento", "taxa", "pu"),
    )

    assert resultado["duration"][0] == pytest.approx(
        ntnb.duration(data_liquidacao, data_vencimento, taxa)
    )
    assert resultado["dv01"][0] == pytest.approx(
        ntnb.dv01(data_liquidacao, data_vencimento, taxa, pu)
    )


def test_ntnc_exprs_batem_com_calculos_escalares():
    data_liquidacao = "21-03-2025"
    data_vencimento = "01-01-2031"
    taxa = 0.067626
    vna = 6598.913723
    pu = ntnc.pu(vna, ntnc.cotacao(data_liquidacao, data_vencimento, taxa))

    resultado = pl.DataFrame(
        {
            "data_liquidacao": [data_liquidacao],
            "data_vencimento": [data_vencimento],
            "taxa": [taxa],
            "pu": [pu],
        }
    ).select(
        duration=ntnc.duration_expr("data_liquidacao", "data_vencimento", "taxa"),
        dv01=ntnc.dv01_expr("data_liquidacao", "data_vencimento", "taxa", "pu"),
    )

    assert resultado["duration"][0] == pytest.approx(
        ntnc.duration(data_liquidacao, data_vencimento, taxa)
    )
    assert resultado["dv01"][0] == pytest.approx(
        ntnc.dv01(data_liquidacao, data_vencimento, taxa, pu)
    )


R_MAIS = ntnb1.NomeComercial.RENDA_MAIS
FUNCOES_COM_TAXA = [
    lambda taxa: ntnb.duration("05-09-2024", "15-08-2040", taxa),
    lambda taxa: ntnc.duration("21-03-2025", "01-01-2031", taxa),
    lambda taxa: ntnf.duration("02-09-2024", "01-01-2035", taxa),
    lambda taxa: ntnb1.duration("23-06-2025", "15-12-2084", taxa, R_MAIS),
    lambda taxa: ntnb1.dv01("23-06-2025", "15-12-2084", taxa, 100.0, R_MAIS),
    lambda taxa: ntnbp.dv01("03-01-2025", "15-05-2035", taxa, 2000.0),
    lambda taxa: ltn.rentabilidade(taxa, Decimal("0.11725")),
    lambda taxa: lft.rentabilidade(taxa, Decimal("0.11725")),
    lambda taxa: ntnf.rentabilidade(
        "23-08-2024", "01-01-2035", taxa, ["2025-01-01", "2035-01-01"], [0.108, 0.115]
    ),
    lambda taxa: ntnf.premio_limpo(
        "23-08-2024", "01-01-2035", taxa, ["2025-01-01", "2035-01-01"], [0.108, 0.115]
    ),
]


@pytest.mark.parametrize("funcao", FUNCOES_COM_TAXA)
def test_taxa_decimal_equivale_a_float_e_a_percentual(funcao):
    """A taxa pode ser float, Decimal ou percentual textual, com o mesmo resultado."""
    esperado = funcao(0.0625)

    assert math.isfinite(esperado)
    assert funcao(Decimal("0.0625")) == esperado
    assert funcao("6.25%") == esperado
