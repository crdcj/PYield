import datetime as dt

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import pyield as yd


def _preco(data, vencimento, taxa):
    fluxos = yd.ntnf.fluxos_caixa(data, vencimento)
    dias = yd.du.contar(data, fluxos["data_pagamento"])
    return sum(
        cf / (1 + (taxa(t) if callable(taxa) else taxa)) ** (t / 252)
        for cf, t in zip(fluxos["valor_pagamento"], dias)
    )


def test_curva_conjunta_preserva_ltn_e_reprecifica_ntnf():
    data = "04-09-2026"
    curva = yd.ntnf.taxas_zero_forwards(
        data,
        ["01-01-2032", "01-01-2029", "01-01-2030"],
        [0.142765, 0.13915, 0.14107],
        ["01-01-2033", "01-01-2029", "01-01-2031", "01-01-2027"],
        [0.145, 0.50, 0.143, 0.13],
    )
    interpolar = yd.Interpolador(
        curva["dias_uteis"], curva["taxa_zero"], "flat_forward"
    )
    for vencimento, taxa in [
        ("01-01-2029", 0.13915),
        ("01-01-2030", 0.14107),
        ("01-01-2032", 0.142765),
    ]:
        assert interpolar(yd.du.contar(data, vencimento)) == pytest.approx(
            taxa, abs=1e-14
        )
    for vencimento, tir in [
        ("01-01-2027", 0.13),
        ("01-01-2031", 0.143),
        ("01-01-2033", 0.145),
    ]:
        assert _preco(data, vencimento, interpolar) == pytest.approx(
            _preco(data, vencimento, tir), abs=1e-8, rel=0
        )
    assert curva.height == 6  # noqa: PLR2004
    assert yd.tpf.ntnf.taxas_zero_forwards is yd.ntnf.taxas_zero_forwards


@pytest.mark.parametrize("taxa", [-0.02, 0.0, 0.12])
def test_curva_plana_sem_ltn_e_cupons(taxa):
    args = ("04-09-2026", [], [], ["01-01-2027", "01-01-2031"], [taxa, taxa])
    curva = yd.ntnf.taxas_zero_forwards(*args)
    cupons = yd.ntnf.taxas_zero_forwards(*args, incluir_cupons=True)
    assert curva["taxa_zero"].to_list() == pytest.approx([taxa, taxa], abs=1e-11)
    assert cupons.height > curva.height
    assert cupons["taxa_zero"].to_list() == pytest.approx(
        [taxa] * cupons.height, abs=1e-11
    )
    assert_frame_equal(
        cupons.filter(
            pl.col("data_vencimento").is_in(curva["data_vencimento"].implode())
        ),
        curva,
    )


def test_apenas_ltn_e_vencidos():
    curva = yd.ntnf.taxas_zero_forwards(
        "04-09-2026", ["01-01-2026", "01-01-2032"], [0.5, 0.14], [], []
    )
    assert curva["data_vencimento"].to_list() == [dt.date(2032, 1, 1)]
    assert curva["taxa_zero"].to_list() == [0.14]
    vazio = yd.ntnf.taxas_zero_forwards("04-09-2026", [], [], [], [])
    assert vazio.is_empty()
    assert vazio.schema == curva.schema


@pytest.mark.parametrize(
    ("datas", "taxas"),
    [
        (["01-01-2030"], []),
        (["01-01-2030"] * 2, [0.1, 0.2]),
        (["01-01-2030"], [float("nan")]),
        (["01-01-2030"], [-1.0]),
        (["01-01-2030"], [None]),
    ],
)
def test_entradas_invalidas(datas, taxas):
    with pytest.raises(ValueError, match="tamanho|duplicados|válidas"):
        yd.ntnf.taxas_zero_forwards("04-09-2026", datas, taxas, [], [])


def test_preco_incompativel():
    with pytest.raises(ValueError, match="incompatível"):
        yd.ntnf.taxas_zero_forwards(
            "04-09-2026", ["01-01-2030"], [-0.5], ["01-01-2031"], [0.9]
        )
