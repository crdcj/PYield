import datetime as dt

import polars as pl
import pytest

from pyield import Interpolador, interpolar

DATA = dt.date(2025, 1, 2)


def _interpolar_em_uma_data(
    dus_alvo, dus_curva, taxas_curva, metodo="flat_forward"
) -> pl.Series:
    """Chama ``interpolar`` com todos os pontos e vértices na mesma data."""
    alvo, curva = pl.Series(dus_alvo), pl.Series(dus_curva)
    return interpolar(
        datas_alvo=pl.repeat(DATA, alvo.len(), eager=True),
        dus_alvo=alvo,
        datas_curva=pl.repeat(DATA, curva.len(), eager=True),
        dus_curva=curva,
        taxas_curva=pl.Series(taxas_curva),
        metodo=metodo,
    )


def test_interpolador_rejeita_curva_sem_vertices_validos():
    with pytest.raises(ValueError, match="ao menos um vértice válido"):
        Interpolador([1, 2], [None, float("nan")], "flat_forward")


@pytest.mark.parametrize("metodo", ["flat_forward", "linear"])
def test_sequencia_expr_e_escalar_dao_a_mesma_taxa(metodo):
    dus_curva, taxas = [30, 60, 90], [0.045, 0.05, 0.055]
    interp = Interpolador(dus_curva, taxas, metodo)
    dus = [0, 15, 30, 45, 75, 90]
    esperado = [interp(du) for du in dus]
    assert interp(dus).to_list() == esperado
    assert interp.interpolar(dus).to_list() == esperado
    df = pl.DataFrame({"du": dus}).select(taxa=interp.interpolar_expr("du"))
    assert df["taxa"].to_list() == esperado
    em_lote = _interpolar_em_uma_data(dus, dus_curva, taxas, metodo)
    assert em_lote.to_list() == pytest.approx(esperado, rel=1e-12)


def test_interpolar_em_lote_rejeita_metodo_desconhecido():
    with pytest.raises(ValueError, match="'cubico' não reconhecido"):
        _interpolar_em_uma_data([45], [30, 60], [0.045, 0.05], "cubico")


def test_du_sem_taxa_e_nulo_em_todos_os_caminhos_vetoriais():
    dus_curva, taxas = [30, 60, 90], [0.045, 0.05, 0.055]
    alvos = pl.Series([-10, None, 100, 45])
    interp = Interpolador(dus_curva, taxas, "flat_forward")
    esperado = [None, None, None, interp(45)]
    assert interp.interpolar(alvos).to_list() == esperado
    df = pl.DataFrame({"du": alvos}).select(taxa=interp.interpolar_expr("du"))
    assert df["taxa"].to_list() == esperado
    assert _interpolar_em_uma_data(alvos, dus_curva, taxas).to_list() == esperado


@pytest.mark.parametrize("metodo", ["flat_forward", "linear"])
def test_vertice_com_du_negativo_e_ignorado(metodo):
    dus_curva, taxas = [-10, 30, 60], [0.04, 0.05, 0.055]
    sem_negativo = Interpolador(dus_curva[1:], taxas[1:], metodo)
    interp = Interpolador(dus_curva, taxas, metodo)
    alvos = [0, 10, 30, 45]
    esperado = [sem_negativo(du) for du in alvos]
    assert [interp(du) for du in alvos] == esperado
    assert interp(alvos).to_list() == esperado
    em_lote = _interpolar_em_uma_data(alvos, dus_curva, taxas, metodo)
    assert em_lote.to_list() == pytest.approx(esperado, rel=1e-12)


def test_curva_so_com_du_negativo_nao_tem_vertice_valido():
    with pytest.raises(ValueError, match="ao menos um vértice válido"):
        Interpolador([-10, -5], [0.04, 0.05], "flat_forward")
    em_lote = _interpolar_em_uma_data([10], [-10, -5], [0.04, 0.05])
    assert em_lote.to_list() == [None]


def test_sequencia_vazia_retorna_serie_vazia():
    interp = Interpolador([30, 60], [0.045, 0.05], "flat_forward")
    resultado = interp.interpolar([])
    assert resultado.dtype == pl.Float64
    assert resultado.is_empty()


@pytest.mark.parametrize("prazos", [pl.Series([45.0, 46.0]), [45.5], pl.Series(["45"])])
def test_prazos_de_tipo_nao_inteiro_geram_erro(prazos):
    dus_curva, taxas = pl.Series([30, 60, 90]), pl.Series([0.045, 0.05, 0.055])
    interp = Interpolador(dus_curva, taxas, "flat_forward")
    with pytest.raises(TypeError, match="deve ter tipo inteiro"):
        interp.interpolar(prazos)
    with pytest.raises(TypeError, match="deve ter tipo inteiro"):
        pl.DataFrame({"du": prazos}).select(interp.interpolar_expr("du"))
    with pytest.raises(TypeError, match="dus_alvo deve ter tipo inteiro"):
        _interpolar_em_uma_data(prazos, dus_curva, taxas)
    with pytest.raises(TypeError, match="dias_uteis deve ter tipo inteiro"):
        Interpolador(prazos, [0.05] * len(prazos), "flat_forward")
    with pytest.raises(TypeError, match="dus_curva deve ter tipo inteiro"):
        _interpolar_em_uma_data([45], prazos, [0.05] * len(prazos))


def test_prazos_nulos_ou_vazios_nao_geram_erro_de_tipo():
    interp = Interpolador([30, 60, 90], [0.045, 0.05, 0.055], "flat_forward")
    assert interp.interpolar([None, None]).to_list() == [None, None]
    resultado = _interpolar_em_uma_data([None, None], [30, 60], [0.045, 0.05])
    assert resultado.to_list() == [None, None]


def test_interpolar_rejeita_du_nao_inteiro():
    interp = Interpolador([30, 60], [0.045, 0.05], "flat_forward")
    with pytest.raises(TypeError, match="du deve ser int"):
        interp.interpolar(45.5)


DUS_REPETIDOS = [100, 200, 200, 300]


def test_vertice_repetido_identico_e_considerado_uma_vez():
    taxas = [0.10, 0.11, 0.11, 0.12]
    sem_repeticao = Interpolador([100, 200, 300], [0.10, 0.11, 0.12], "flat_forward")
    assert Interpolador(DUS_REPETIDOS, taxas, "flat_forward")(250) == sem_repeticao(250)
    resultado = _interpolar_em_uma_data([250], DUS_REPETIDOS, taxas)
    assert resultado.to_list() == [sem_repeticao(250)]


def test_mesmo_prazo_com_taxas_diferentes_gera_erro():
    taxas = [0.10, 0.11, 0.13, 0.12]
    with pytest.raises(ValueError, match=r"DU 200: taxas \[0.11, 0.13\]"):
        Interpolador(DUS_REPETIDOS, taxas, "flat_forward")
    with pytest.raises(ValueError, match=r"DU 200: taxas \[0.11, 0.13\]"):
        _interpolar_em_uma_data([250], DUS_REPETIDOS, taxas)


def test_conflito_multicurva_informa_a_data():
    d1, d2 = dt.date(2025, 1, 2), dt.date(2025, 1, 3)
    with pytest.raises(ValueError, match=r"2025-01-03, DU 200: taxas \[0.11, 0.13\]"):
        interpolar(
            datas_alvo=pl.Series([d1, d2]),
            dus_alvo=pl.Series([250, 250]),
            datas_curva=pl.Series([d1, d1, d2, d2, d2]),
            dus_curva=pl.Series([200, 300, 200, 200, 300]),
            taxas_curva=pl.Series([0.11, 0.12, 0.11, 0.13, 0.12]),
            metodo="flat_forward",
        )


def test_mesmo_prazo_em_datas_diferentes_nao_e_conflito():
    d1, d2 = dt.date(2025, 1, 2), dt.date(2025, 1, 3)
    resultado = interpolar(
        datas_alvo=pl.Series([d1, d2]),
        dus_alvo=pl.Series([200, 200]),
        datas_curva=pl.Series([d1, d1, d2, d2]),
        dus_curva=pl.Series([200, 300, 200, 300]),
        taxas_curva=pl.Series([0.11, 0.12, 0.13, 0.14]),
        metodo="flat_forward",
    )
    assert resultado.to_list() == [0.11, 0.13]
