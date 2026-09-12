import datetime as dt
import math
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_series_equal

import pyield as yd
from pyield import ntnb1, ntnbp

DATA_LIQUIDACAO = dt.date(2026, 7, 13)
DIRETORIO_DADOS = Path(__file__).parent / "data"
CURVA_PLANILHA = pl.read_csv(
    DIRETORIO_DADOS / "ntnb_curva_zero_20260713.csv", try_parse_dates=True
)
VENCIMENTOS = CURVA_PLANILHA["data_vencimento"]
TAXAS_TIR = CURVA_PLANILHA["taxa_tir"]
TAXAS_ZERO_PLANILHA = CURVA_PLANILHA["taxa_zero"]
FORWARDS_PLANILHA = CURVA_PLANILHA["taxa_forward"]
CASAS_DECIMAIS = 6


def test_namespace_dos_titulos_separado_do_ntnb_anbima():
    assert not hasattr(yd.ntnb, "taxas_zero_td")
    assert not hasattr(yd.ntnbp, "taxas_zero")
    assert yd.ntnb1 is ntnb1
    assert yd.ntnbp is ntnbp


def test_cotacao_e_pu_reproduzem_dtbase():
    """Reproduz a precificação da NTN-B 150826 exibida no dtbase."""
    cotacao_esperada = Decimal("1.029056")
    pu_esperado = Decimal("4880.439369")
    cotacao = yd.ntnb.cotacao("14-08-2026", "15-08-2026", 0.132098)

    assert cotacao == cotacao_esperada
    assert yd.ntnb.pu(4742.6373, cotacao) == pu_esperado


def test_cotacao_e_pu_aceitam_decimal() -> None:
    cotacao = yd.ntnb.cotacao("31-05-2024", "15-05-2035", Decimal("0.061490"))

    assert cotacao == Decimal("0.993651")
    assert cotacao.as_tuple().exponent == -CASAS_DECIMAIS
    assert yd.ntnb.pu(Decimal("4299.160173"), cotacao) == Decimal("4271.864805")


def test_cotacao_e_pu_nulos_retornam_decimal_nan() -> None:
    assert yd.ntnb.cotacao(None, "15-05-2035", Decimal("0.061490")).is_nan()
    assert yd.ntnb.pu(Decimal("NaN"), Decimal("0.993651")).is_nan()


def test_ntnbp_cotacao_e_pu_retornam_decimal() -> None:
    cotacao = yd.ntnbp.cotacao("02-12-2025", "15-05-2029", Decimal("0.0777"))

    assert cotacao == Decimal("0.774630")
    assert cotacao.as_tuple().exponent == -CASAS_DECIMAIS
    assert yd.ntnbp.pu(Decimal("4567.033825"), cotacao) == Decimal("3537.761411")


def test_ntnbp_cotacao_e_pu_nulos_retornam_decimal_nan() -> None:
    assert yd.ntnbp.cotacao(None, "15-05-2029", Decimal("0.0777")).is_nan()
    assert yd.ntnbp.pu(Decimal("NaN"), Decimal("0.774630")).is_nan()


@pytest.mark.parametrize("liquidacao", ["15-08-2026", "17-08-2026"])
def test_ntnbp_rejeita_liquidacao_no_ou_apos_vencimento(liquidacao: str) -> None:
    curva_zero = pl.DataFrame({"dias_uteis": [1, 10], "taxa_zero": [0.07, 0.071]})

    cotacao = ntnbp.cotacao(liquidacao, "15-08-2026", 0.07)

    assert cotacao.is_nan()
    assert ntnbp.pu(4742.6373, cotacao).is_nan()
    assert math.isnan(ntnbp.taxa(liquidacao, "15-08-2026", curva_zero))
    assert math.isnan(ntnbp.dv01(liquidacao, "15-08-2026", 0.07, 1_000))


@pytest.mark.parametrize("liquidacao", ["15-08-2026", "17-08-2026"])
def test_ntnb_rejeita_liquidacao_no_ou_apos_vencimento(liquidacao: str) -> None:
    cotacao = yd.ntnb.cotacao(liquidacao, "15-08-2026", 0.132098)

    assert cotacao.is_nan()
    assert yd.ntnb.pu(4742.6373, cotacao).is_nan()


@pytest.mark.parametrize(
    ("nome_comercial", "vencimento", "cotacao_esperada"),
    [
        (ntnb1.NomeComercial.RENDA_MAIS, "15-12-2084", Decimal("0.038332")),
        (ntnb1.NomeComercial.EDUCA_MAIS, "15-12-2069", Decimal("0.059246")),
    ],
)
def test_ntnb1_cotacao_retorna_decimal(
    nome_comercial: ntnb1.NomeComercial,
    vencimento: str,
    cotacao_esperada: Decimal,
) -> None:
    cotacao = ntnb1.cotacao(
        "18-06-2025",
        vencimento,
        Decimal("0.07010"),
        nome_comercial,
    )

    assert cotacao == cotacao_esperada
    assert cotacao.as_tuple().exponent == -CASAS_DECIMAIS


def test_ntnb1_pu_retorna_decimal() -> None:
    assert ntnb1.pu(Decimal("4299.160173"), Decimal("0.993651")) == Decimal(
        "4271.864805"
    )


def test_ntnb1_cotacao_e_pu_nulos_retornam_decimal_nan() -> None:
    assert ntnb1.cotacao(
        None,
        "15-12-2084",
        Decimal("0.07010"),
        ntnb1.NomeComercial.RENDA_MAIS,
    ).is_nan()
    assert ntnb1.pu(Decimal("NaN"), Decimal("0.993651")).is_nan()


@pytest.mark.parametrize("data_liquidacao", ["13-07-2026", "15-07-2026", "16-07-2026"])
def test_taxas_zero_retornam_apenas_vencimentos(data_liquidacao):
    resultado = yd.ntnb.taxas_zero(data_liquidacao, ["15-08-2026"], [0.1])
    assert resultado["data_vencimento"].to_list() == [dt.date(2026, 8, 15)]
    assert resultado["taxa_zero"][0] == pytest.approx(0.1)
    assert resultado.columns == [
        "data_vencimento",
        "dias_uteis",
        "taxa_tir",
        "taxa_forward",
        "taxa_zero",
    ]


def test_taxas_zero_reproduz_planilha_curva_zero():
    """A calibração deve reproduzir os vértices da aba Curva Zero."""
    resultado = yd.ntnb.taxas_zero(DATA_LIQUIDACAO, VENCIMENTOS, TAXAS_TIR)

    assert_series_equal(resultado["data_vencimento"], VENCIMENTOS)
    assert_series_equal(resultado["taxa_tir"], TAXAS_TIR, check_exact=True)
    assert_series_equal(
        resultado["taxa_zero"], TAXAS_ZERO_PLANILHA, abs_tol=1e-8, rel_tol=0
    )
    assert_series_equal(
        resultado["taxa_forward"], FORWARDS_PLANILHA, abs_tol=1e-8, rel_tol=0
    )


def test_taxas_zero_limita_busca_sem_intervalo():
    taxas = TAXAS_TIR.clone()
    taxas[2] = 10.0

    with pytest.raises(RuntimeError, match="encontrar um intervalo"):
        yd.ntnb.taxas_zero(DATA_LIQUIDACAO, VENCIMENTOS, taxas)


def test_taxas_zero_retornam_vazio_sem_vencimentos_futuros() -> None:
    liquidacao = "17-08-2026"
    vencimentos = ["15-08-2026"]
    taxas = [0.07]

    curva = yd.ntnb.taxas_zero(liquidacao, vencimentos, taxas)
    assert curva.is_empty()
    assert curva.schema == {
        "data_vencimento": pl.Date,
        "dias_uteis": pl.Int64,
        "taxa_tir": pl.Float64,
        "taxa_forward": pl.Float64,
        "taxa_zero": pl.Float64,
    }


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
def test_taxas_zero_reproduz_taxas_ntnb_principal(
    vencimento,
    taxa_mercado,
    taxa_compra,
    taxa_venda,
):
    curva = yd.ntnb.taxas_zero(
        DATA_LIQUIDACAO,
        VENCIMENTOS,
        TAXAS_TIR,
    )
    taxa_zero = ntnbp.taxa(DATA_LIQUIDACAO, vencimento, curva)

    assert round(taxa_zero, 4) == taxa_mercado
    assert round(taxa_mercado - 0.0004, 4) == taxa_compra
    assert round(taxa_mercado + 0.0008, 4) == taxa_venda


def test_ntnb1_cotacao_curva_zero_reproduz_planilha_td():
    curva_zero = yd.ntnb.taxas_zero(
        DATA_LIQUIDACAO,
        VENCIMENTOS,
        TAXAS_TIR,
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


@pytest.mark.parametrize(
    ("vencimentos", "taxas", "tolerancia"),
    [
        (VENCIMENTOS, TAXAS_TIR, 2e-12),
        # Trechos longos amplificam no preço a tolerância de 1e-12 do forward.
        ([VENCIMENTOS[1], VENCIMENTOS[-1]], [0.06, 0.07], 1e-10),
        ([VENCIMENTOS[1], VENCIMENTOS[-1]], [0.0, 0.0], 1e-10),
        ([VENCIMENTOS[1], VENCIMENTOS[-1]], [-0.01, -0.005], 1e-10),
    ],
)
def test_curva_zero_interpolada_reproduz_cotacoes_dos_titulos(
    vencimentos, taxas, tolerancia
):
    curva = yd.ntnb.taxas_zero(DATA_LIQUIDACAO, vencimentos, taxas)
    interpolar = yd.Interpolador(
        curva["dias_uteis"], curva["taxa_zero"], metodo="flat_forward"
    )
    for vencimento, tir in zip(vencimentos, taxas, strict=True):
        fluxos = yd.ntnb.fluxos_caixa(DATA_LIQUIDACAO, vencimento)
        prazos = yd.du.contar(DATA_LIQUIDACAO, fluxos["data_pagamento"])
        cotacao_tir = sum(
            float(valor) / (1 + tir) ** (prazo / 252)
            for valor, prazo in zip(fluxos["valor_pagamento"], prazos, strict=True)
        )
        cotacao_curva = sum(
            float(valor) / (1 + interpolar(prazo)) ** (prazo / 252)
            for valor, prazo in zip(fluxos["valor_pagamento"], prazos, strict=True)
        )
        assert cotacao_curva == pytest.approx(cotacao_tir, abs=tolerancia, rel=0)


def test_forwards_derivados_das_zeros_reproduzem_planilha():
    curva = yd.ntnb.taxas_zero(DATA_LIQUIDACAO, VENCIMENTOS, TAXAS_TIR)
    forwards = curva.select(
        taxa_forward=yd.forwards_expr("dias_uteis", "taxa_zero")
    ).to_series()
    assert_series_equal(curva["taxa_forward"], forwards, abs_tol=1e-12, rel_tol=0)
    assert_series_equal(forwards, FORWARDS_PLANILHA, abs_tol=1e-8, rel_tol=0)
