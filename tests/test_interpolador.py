import datetime as dt

import polars as pl
import pytest

from pyield import Interpolador, interpolar


def test_interpolador_rejeita_curva_sem_vertices_validos():
    with pytest.raises(ValueError, match="ao menos um vértice válido"):
        Interpolador([1, 2], [None, float("nan")], "flat_forward")


DUS_REPETIDOS = [100, 200, 200, 300]


def test_vertice_repetido_identico_e_considerado_uma_vez():
    taxas = [0.10, 0.11, 0.11, 0.12]
    sem_repeticao = Interpolador([100, 200, 300], [0.10, 0.11, 0.12], "flat_forward")
    assert Interpolador(DUS_REPETIDOS, taxas, "flat_forward")(250) == sem_repeticao(250)
    resultado = interpolar(pl.Series([250]), pl.Series(DUS_REPETIDOS), pl.Series(taxas))
    assert resultado.to_list() == [sem_repeticao(250)]


def test_mesmo_prazo_com_taxas_diferentes_gera_erro():
    taxas = [0.10, 0.11, 0.13, 0.12]
    with pytest.raises(ValueError, match=r"DU 200: taxas \[0.11, 0.13\]"):
        Interpolador(DUS_REPETIDOS, taxas, "flat_forward")
    with pytest.raises(ValueError, match=r"DU 200: taxas \[0.11, 0.13\]"):
        interpolar(pl.Series([250]), pl.Series(DUS_REPETIDOS), pl.Series(taxas))


def test_conflito_multicurva_informa_a_data():
    d1, d2 = dt.date(2025, 1, 2), dt.date(2025, 1, 3)
    with pytest.raises(ValueError, match=r"2025-01-03, DU 200: taxas \[0.11, 0.13\]"):
        interpolar(
            pl.Series([250, 250]),
            pl.Series([200, 300, 200, 200, 300]),
            pl.Series([0.11, 0.12, 0.11, 0.13, 0.12]),
            datas_alvo=pl.Series([d1, d2]),
            datas_curva=pl.Series([d1, d1, d2, d2, d2]),
        )


def test_mesmo_prazo_em_datas_diferentes_nao_e_conflito():
    d1, d2 = dt.date(2025, 1, 2), dt.date(2025, 1, 3)
    resultado = interpolar(
        pl.Series([200, 200]),
        pl.Series([200, 300, 200, 300]),
        pl.Series([0.11, 0.12, 0.13, 0.14]),
        datas_alvo=pl.Series([d1, d2]),
        datas_curva=pl.Series([d1, d1, d2, d2]),
    )
    assert resultado.to_list() == [0.11, 0.13]
