import datetime as dt
from decimal import Decimal
from pathlib import Path

import pytest

import pyield as yd
from pyield.anbima import projecoes

FIXTURE = Path(__file__).parent / "data" / "anbima_projecoes_20261112.html"


@pytest.fixture
def pagina(monkeypatch):
    monkeypatch.setattr(projecoes, "_buscar_projecoes", FIXTURE.read_bytes)


def _fixar_hoje(monkeypatch, data: dt.date):
    momento = dt.datetime.combine(data, dt.time(10), tzinfo=yd.relogio.BR_TZ)
    monkeypatch.setattr(yd.relogio, "agora", lambda: momento)


def test_api_publica():
    assert yd.ipca.taxa_projetada is projecoes.ipca_taxa_projetada
    assert yd.igpm.taxa_projetada is projecoes.igpm_taxa_projetada


@pytest.mark.parametrize(
    ("funcao", "hoje", "esperado"),
    [
        pytest.param(
            yd.igpm.taxa_projetada,
            dt.date(2026, 11, 12),
            (
                dt.date(2026, 11, 1),
                "0.0078",
                dt.date(2026, 11, 11),
                dt.date(2026, 11, 12),
            ),
            id="igpm-coleta-de-ontem-vale-hoje",
        ),
        pytest.param(
            yd.igpm.taxa_projetada,
            dt.date(2026, 11, 11),
            (
                dt.date(2026, 11, 1),
                "0.0087",
                dt.date(2026, 10, 29),
                dt.date(2026, 11, 4),
            ),
            id="igpm-coleta-do-dia-ainda-nao-vale",
        ),
        pytest.param(
            yd.ipca.taxa_projetada,
            dt.date(2026, 11, 12),
            (
                dt.date(2026, 10, 1),
                "0.0021",
                dt.date(2026, 10, 23),
                dt.date(2026, 10, 26),
            ),
            id="ipca",
        ),
    ],
)
def test_projecao_vigente(monkeypatch, pagina, funcao, hoje, esperado):
    _fixar_hoje(monkeypatch, hoje)

    resultado = funcao()

    assert resultado is not None
    competencia, valor, coleta, validade = esperado
    assert resultado.competencia == competencia
    assert isinstance(resultado.valor_projetado, Decimal)
    assert resultado.valor_projetado.as_tuple() == Decimal(valor).as_tuple()
    assert resultado.data_coleta == coleta
    assert resultado.data_validade == validade


def test_sem_projecao_vigente_retorna_none(monkeypatch, pagina):
    # Entre o IGP-M fechado de outubro e a validade da primeira projeção
    # de novembro (04/11/2026), nenhuma projeção do mês está vigente.
    _fixar_hoje(monkeypatch, dt.date(2026, 11, 3))

    assert yd.igpm.taxa_projetada() is None


def test_resposta_vazia_e_erro(monkeypatch):
    monkeypatch.setattr(projecoes, "_buscar_projecoes", lambda: b"")

    with pytest.raises(ValueError, match="sem conteúdo"):
        yd.ipca.taxa_projetada()
