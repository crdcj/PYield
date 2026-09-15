import math

import polars as pl
import pytest

import pyield as yd
from pyield.tpf.titulos import _utils as utils  # noqa: PLC2701


@pytest.mark.parametrize("raiz", [0.0, 0.01, 0.02, -0.05, 0.15])
def test_busca_automatica(raiz):
    assert utils.encontrar_raiz(lambda x: x - raiz) == pytest.approx(raiz)


@pytest.mark.parametrize("raiz", [0.0, 0.3, 1.0])
def test_intervalo_explicito_e_extremidades(raiz):
    assert utils.encontrar_raiz(
        lambda x: x - raiz, intervalo=(0.0, 1.0)
    ) == pytest.approx(raiz)


def test_intervalo_pontual_com_raiz():
    raiz = 2.0
    assert utils.encontrar_raiz(lambda x: x - raiz, intervalo=(raiz, raiz)) == raiz


@pytest.mark.parametrize("intervalo", [(1, 0), (math.nan, 1), (0, math.inf)])
def test_limites_invalidos(intervalo):
    with pytest.raises(ValueError, match="finitos e ordenados"):
        utils.encontrar_raiz(lambda x: x, intervalo=intervalo)


@pytest.mark.parametrize("intervalo", [None, (0, 1), (1, 1)])
def test_sem_raiz(intervalo):
    assert math.isnan(utils.encontrar_raiz(lambda x: x * x + 1, intervalo=intervalo))


@pytest.mark.parametrize("valor", [math.nan, math.inf, -math.inf])
@pytest.mark.parametrize("intervalo", [None, (0, 1)])
def test_avaliacao_nao_finita(valor, intervalo):
    assert math.isnan(utils.encontrar_raiz(lambda x: valor, intervalo=intervalo))


def test_avaliacao_nao_finita_no_meio():
    assert math.isnan(
        utils.encontrar_raiz(lambda x: math.nan if x == 0 else x, intervalo=(-1, 1))
    )


def test_limite_de_iteracoes():
    assert math.isnan(
        utils.encontrar_raiz(lambda x: -1 if x < 1 else 1, intervalo=(0, 1e100))
    )


@pytest.mark.parametrize("titulo", [yd.lft, yd.ntnb, yd.ntnc, yd.ntnf])
def test_taxa_publica_propaga_falha_e_preserva_ausencia(titulo):
    vencimento = "01-01-2031" if titulo in {yd.ntnf, yd.ntnc} else "15-05-2035"
    args = ("21-03-2025", vencimento)
    if titulo is not yd.ntnf:
        args += (5000.0,)
    assert math.isnan(titulo.taxa(*args, None))
    assert math.isnan(titulo.taxa(*args, 1e100))


def test_ntnb_forward_fora_do_intervalo():
    with pytest.raises(RuntimeError, match="intervalo"):
        yd.ntnb.taxas_zero(
            "21-03-2025", ["15-05-2026", "15-05-2035"], [0.1, -0.995]
        )


def test_ntnb1_taxa_fora_do_intervalo():
    curva = pl.DataFrame(
        {"dias_uteis": [1, 30000], "taxa_zero": [-0.995, -0.995]}
    )
    assert math.isnan(
        yd.ntnb1.taxa_curva_zero(
            "23-06-2025", "15-12-2084", curva, yd.ntnb1.NomeComercial.RENDA_MAIS
        )
    )


@pytest.mark.parametrize("valor", [math.nan, math.inf])
def test_bootstrap_ntnb_rejeita_falha_do_solver(monkeypatch, valor):
    monkeypatch.setattr(utils, "encontrar_raiz", lambda *args, **kwargs: valor)
    with pytest.raises(RuntimeError, match="calibrar a NTN-B"):
        yd.ntnb.taxas_zero(
            "21-03-2025", ["15-05-2026", "15-05-2035"], [0.1, 0.11]
        )


def test_solver_propaga_erro_inesperado():
    def erro(x):
        raise RuntimeError("erro inesperado")

    with pytest.raises(RuntimeError, match="erro inesperado"):
        utils.encontrar_raiz(erro)
