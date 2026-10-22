"""Reconstrução do VNA-base da NTN-B com dados oficiais preservados.

O JSON do IBGE foi salvo byte a byte em 22/10/2026, da URL:
https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/200006-202609/variaveis/2266?localidades=N1[all]
O Parquet do Tesouro já existente permanece inalterado.
"""

import datetime as dt
import json
from decimal import Decimal, localcontext
from pathlib import Path

import polars as pl
import pytest

import pyield as yd
from pyield.vna import _calculo as calculo  # noqa: PLC2701
from pyield.vna import _consulta as consulta  # noqa: PLC2701

DADOS = Path(__file__).parent / "data"
REFERENCIAS = pl.read_parquet(DADOS / "vna_ntnb.parquet")
INDICES = {
    periodo: Decimal(valor)
    for periodo, valor in json.loads(
        (DADOS / "ipca_indices_200006_202609.json").read_bytes()
    )[0]["resultados"][0]["series"][0]["serie"].items()
}
DATA_DIVERGENTE = dt.date(2014, 2, 15)
EXPOENTE_VNA = -6


def _indice(data: dt.date) -> Decimal:
    competencia = data.replace(day=1) - dt.timedelta(days=1)
    return INDICES[competencia.strftime("%Y%m")]


@pytest.mark.parametrize(
    ("data", "publicado"),
    REFERENCIAS.filter(pl.col("data") != DATA_DIVERGENTE).rows(),
)
def test_reconstroi_referencias_publicadas(data, publicado):
    resultado = yd.vna.calcular_base("NTN-B", _indice(data))

    assert isinstance(resultado, Decimal)
    assert resultado == publicado
    assert resultado.as_tuple().exponent == EXPOENTE_VNA


def test_registra_divergencia_da_publicacao_em_fevereiro_2014():
    """Expõe a divergência entre fontes; não altera nem tolera a referência."""
    publicado = REFERENCIAS.filter(pl.col("data") == DATA_DIVERGENTE).item(0, "vna")

    calculado = yd.vna.calcular_base("NTN-B", _indice(DATA_DIVERGENTE))

    assert publicado == Decimal("2376.026557")
    assert calculado == Decimal("2376.020363")
    assert calculado != publicado


def test_base_anterior_truncada_perde_precisao():
    anterior = REFERENCIAS.filter(pl.col("data") == dt.date(2000, 9, 15)).item(0, "vna")
    publicado = REFERENCIAS.filter(pl.col("data") == dt.date(2000, 10, 15)).item(0, "vna")

    calculado = calculo.calcular_base(anterior, INDICES["200008"], INDICES["200009"])

    assert calculado == Decimal("1031.778374")
    assert publicado == Decimal("1031.778375")


def test_encadear_bases_truncadas_acumula_perda():
    base = REFERENCIAS.item(0, "vna")
    datas = REFERENCIAS["data"].to_list()
    for anterior, atual in zip(datas, datas[1:]):
        base = calculo.calcular_base(base, _indice(anterior), _indice(atual))

    assert base == Decimal("4766.334768")
    assert REFERENCIAS.item(-1, "vna") == Decimal("4766.335112")


def test_aceita_float_e_independe_do_contexto_decimal():
    with localcontext(prec=9):
        resultado = yd.vna.calcular_base("NTN-B", 7657.73)

    assert resultado == Decimal("4742.744422")


def test_trunca_fator_antes_de_multiplicar_e_trunca_resultado():
    # Sem T16, o resultado seria 100000000000.000001; arredondar também difere.
    resultado = calculo.calcular_base(
        Decimal("100000000000"), Decimal("1"), Decimal("1.000000000000000019")
    )

    assert resultado == Decimal("100000000000.000000")
    assert calculo.calcular_base(1000.0, 1.0, 1.0000000019) == Decimal("1000.000001")


@pytest.mark.parametrize("parametro", ["vna_base", "indice_base", "indice_atual"])
@pytest.mark.parametrize("valor", [0.0, -1.0, float("inf"), float("nan")])
def test_rejeita_valores_nao_positivos_ou_nao_finitos(parametro, valor):
    entradas = {"vna_base": 1000.0, "indice_base": 1614.62, "indice_atual": 7657.73}
    entradas[parametro] = valor

    with pytest.raises(ValueError, match="positivo e finito"):
        calculo.calcular_base(**entradas)


@pytest.mark.parametrize("valor", [0.0, -1.0, float("inf"), float("nan")])
@pytest.mark.parametrize(
    ("titulo", "vencimento"),
    [("LFT", None), ("NTN-B", None), ("NTN-C", "01-01-2031"), ("NTN-C", "01-01-2006")],
)
def test_calculo_publico_rejeita_indice_invalido(titulo, vencimento, valor):
    with pytest.raises(ValueError, match="positivo e finito"):
        yd.vna.calcular_base(titulo, valor, vencimento=vencimento)


@pytest.mark.parametrize("titulo", ["NTN-F", "invalido"])
def test_calculo_publico_rejeita_titulo_invalido(titulo):
    with pytest.raises(ValueError, match="LFT, NTN-B ou NTN-C"):
        yd.vna.calcular_base(titulo, 7657.73)


@pytest.mark.parametrize("vencimento", [None, "", "31-02-2031"])
def test_calculo_ntnc_exige_vencimento_valido(vencimento):
    with pytest.raises(ValueError, match="vencimento|Data inválida"):
        yd.vna.calcular_base("NTN-C", 1225.846, vencimento=vencimento)


@pytest.mark.parametrize("titulo", ["NTN-B", "LFT"])
def test_calculo_sem_serie_de_vencimento_rejeita_vencimento(titulo):
    with pytest.raises(ValueError, match="apenas à NTN-C"):
        yd.vna.calcular_base(titulo, 7657.73, vencimento="01-01-2031")


def test_calculo_ntnc_sem_serie_retorna_nan():
    assert yd.vna.calcular_base("NTN-C", 1225.846, vencimento="01-01-2040").is_nan()


def test_api_publica_reexporta_implementacao():
    assert "calcular_base" in yd.vna.__all__
    assert yd.vna.calcular_base is consulta.calcular_base
