import polars as pl
import pytest

from pyield import du, lft, ltn, ntnb, ntnc, ntnf


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
    pu = ltn.pu(data_liquidacao, data_vencimento, taxa)

    resultado = pl.DataFrame(
        {
            "data_liquidacao": [data_liquidacao],
            "data_vencimento": [data_vencimento],
            "taxa": [taxa],
            "taxa_di": [0.149],
            "pu": [pu],
        }
    ).select(
        duration=ltn.duration_expr("data_liquidacao", "data_vencimento"),
        rentabilidade=ltn.rentabilidade_expr("taxa", "taxa_di"),
        dv01=ltn.dv01_expr("data_liquidacao", "data_vencimento", "taxa", "pu"),
    )

    assert resultado["duration"][0] == pytest.approx(
        du.contar(data_liquidacao, data_vencimento) / 252
    )
    assert resultado["dv01"][0] == pytest.approx(
        ltn.dv01(data_liquidacao, data_vencimento, taxa, pu)
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
        dv01=ntnf.dv01_expr("data_liquidacao", "data_vencimento", "taxa", "pu"),
    )

    assert resultado["duration"][0] == pytest.approx(
        ntnf.duration(data_liquidacao, data_vencimento, taxa)
    )
    assert resultado["dv01"][0] == pytest.approx(
        ntnf.dv01(data_liquidacao, data_vencimento, taxa, pu)
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
