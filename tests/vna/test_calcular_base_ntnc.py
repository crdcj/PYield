"""Validação de calcular_base com as duas séries oficiais de NTN-C.

Usa o JSON bruto de IGP-M e o Parquet de VNA já preservados no repositório.
A seleção pública do título e do vencimento prepara internamente a origem.
"""

import datetime as dt
from decimal import ROUND_DOWN, Decimal, localcontext
from pathlib import Path

import polars as pl
import pytest

import pyield as yd
from pyield._internal import igpm  # noqa: PLC2701
from pyield.vna import _calculo as calculo  # noqa: PLC2701

DADOS = Path(__file__).parent / "data"


@pytest.fixture
def indices(monkeypatch):
    arquivo = DADOS.parents[1] / "igpm" / "data" / "igpm_ipeadata_202609.json"
    monkeypatch.setattr(igpm, "_buscar_historico", arquivo.read_bytes)
    return dict(yd.igpm.indice_serie("01-11-1999", "30-09-2026").iter_rows())


@pytest.mark.parametrize(
    ("vencimento", "emissao"),
    [(2031, dt.date(2000, 7, 1)), (2006, dt.date(1999, 12, 3))],
)
def test_calculo_publico_reproduz_toda_a_serie_ntnc(indices, vencimento, emissao):
    referencias = pl.read_parquet(DADOS / "vna_ntnc.parquet").filter(
        pl.col("anos_vencimento").list.contains(vencimento)
    )
    with localcontext(prec=28):
        base = Decimal(1000)
        indice_base = indices[200006]
        if emissao.day != 1:
            # Mesma origem do teste histórico de IGP-M: 29 dias de dezembro.
            expoente = (Decimal(29) / 31).quantize(Decimal("1e-14"), rounding=ROUND_DOWN)
            base *= (indices[199912] / indices[199911]) ** expoente
            indice_base = indices[199912]

        for data, _, publicado in referencias.iter_rows():
            competencia = data - dt.timedelta(days=1)
            indice_atual = indices[competencia.year * 100 + competencia.month]

            calculado = yd.vna.calcular_base(
                "NTN-C", indice_atual, vencimento=dt.date(vencimento, 1, 1)
            )

            assert calculado == publicado, data
            assert calculado == calculo.calcular_base(base, indice_base, indice_atual)


def test_primeiro_vna_publicado_da_serie_2006_perde_precisao(indices):
    referencias = pl.read_parquet(DADOS / "vna_ntnc.parquet").filter(
        pl.col("anos_vencimento").list.contains(2006)
    )
    primeira_base = referencias.item(0, "vna")
    publicado = referencias.filter(pl.col("data") == dt.date(2000, 9, 1)).item(0, "vna")

    calculado = calculo.calcular_base(primeira_base, indices[200006], indices[200008])

    assert calculado == Decimal("1091.045593")
    assert publicado == Decimal("1091.045594")


@pytest.mark.parametrize(
    "vencimento", ["01-01-2031", "01/01/2031", "2031-01-01", dt.date(2031, 1, 1)]
)
def test_vencimento_aceita_formatos_da_biblioteca(vencimento):
    assert yd.vna.calcular_base("NTN-C", 1225.846, vencimento) == Decimal("6671.452284")


def test_origem_ntnc_independe_do_contexto_decimal(indices):
    with localcontext(prec=9):
        resultado = yd.vna.calcular_base("NTN-C", indices[200006], "01-01-2006")

    assert resultado == Decimal("1049.125124")
