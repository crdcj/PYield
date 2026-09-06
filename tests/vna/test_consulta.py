import datetime as dt
from decimal import Decimal

import polars as pl
import pytest

import pyield as yd
from pyield.vna import _consulta as consulta  # noqa: PLC2701
from pyield.vna import _ntnb as ntnb  # noqa: PLC2701
from pyield.vna import _ntnc as ntnc  # noqa: PLC2701


@pytest.fixture
def series(monkeypatch):
    datas = [dt.date(2026, 2, 1), dt.date(2026, 1, 1)]
    monkeypatch.setattr(
        ntnb, "vnas", lambda: pl.DataFrame({"data": datas, "vna": [110.0, 100.0]})
    )
    monkeypatch.setattr(
        ntnc,
        "vnas",
        lambda: pl.DataFrame(
            {
                "data": [*datas, datas[1]],
                "vna": [220.0, 200.0, 300.0],
                "anos_vencimento": [[2031], [2031], [2006]],
            }
        ),
    )


@pytest.mark.usefixtures("series")
def test_historico_e_ultimo_por_serie():
    assert yd.vna.historico("NTN-B")["vna"].to_list() == [100.0, 110.0]
    assert yd.vna.ultimo("NTN-B").row(0) == (dt.date(2026, 2, 1), 110.0)
    assert yd.vna.ultimo("NTN-C")["vna"].to_list() == [300.0, 220.0]
    filtrado = yd.vna.ultimo("NTN-C", "01-01-2031")
    assert filtrado["vna"].to_list() == [220.0]
    vazio = yd.vna.ultimo("NTN-C", "01-01-2040")
    assert vazio.is_empty()
    assert vazio.schema == yd.vna.historico("NTN-C").schema


@pytest.mark.usefixtures("series")
def test_valor_preserva_consultas_existentes():
    assert yd.vna.valor("NTN-B", "01-02-2026") == Decimal("110.000000")
    assert yd.vna.valor("NTN-C", "01-02-2026", "01-01-2031") == Decimal("220.000000")
    assert yd.vna.valor("NTN-C", "16-01-2026", "01-01-2031") == ntnc.vna(
        "16-01-2026", "01-01-2031"
    )
    assert yd.vna.valor("NTN-C", "01-02-2026").is_nan()
    assert yd.vna.valor("LFT").is_nan()


def test_projecao_ntnb_aceita_float_e_retorna_decimal():
    resultado = yd.vna.projetado("NTN-B", "30-06-2026", 4731.856412, 0.45)

    assert isinstance(resultado, Decimal)
    assert resultado == Decimal("4742.491138")


def test_projecao_ntnc_com_numeros_comuns():
    resultado = yd.vna.projetado("NTN-C", "16-06-2026", 6693.537239, 0.30)

    assert resultado == Decimal("6703.570025")


def test_projecao_tambem_aceita_decimal():
    resultado = yd.vna.projetado(
        "NTN-C", "16-06-2026", Decimal("6693.537239"), Decimal("0.30")
    )

    assert resultado == Decimal("6703.570025")


@pytest.mark.parametrize("titulo", ["LFT", "LTN", "invalido"])
def test_operacoes_mensais_rejeitam_titulo(titulo):
    with pytest.raises(ValueError, match="NTN-B e NTN-C"):
        yd.vna.ultimo(titulo)
    with pytest.raises(ValueError, match="NTN-B e NTN-C"):
        yd.vna.vigencia(titulo, "01-01-2026")
    with pytest.raises(ValueError, match="NTN-B e NTN-C"):
        yd.vna.projetado(titulo, "01-01-2026", 1000, 0.45)


def test_argumentos_incompativeis():
    with pytest.raises(ValueError, match="Título"):
        yd.vna.valor("LTN", "01-01-2026")
    with pytest.raises(ValueError, match="Vencimento"):
        yd.vna.valor("NTN-B", "01-01-2026", "01-01-2031")
    with pytest.raises(ValueError, match="Vencimento"):
        yd.vna.historico("NTN-B", "01-01-2031")


def test_namespace_reexporta_implementacoes():
    for nome in ("valor", "historico", "ultimo", "vigencia", "projetado"):
        assert getattr(yd.vna, nome) is getattr(consulta, nome)
    for modulo in (yd.ntnb, yd.ntnc, yd.lft):
        for nome in ("vna", "vnas", "vna_projetado", "vigencia"):
            assert not hasattr(modulo, nome)
