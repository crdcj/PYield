import datetime as dt
from decimal import Decimal
from pathlib import Path

import pyield as yd
from pyield.ipca import projetado


def test_taxa_projetada_planilha_anbima(monkeypatch):
    caminho = Path(__file__).parent / "ipca" / "data" / "ipca_indicadores_20261021.xls"
    monkeypatch.setattr(projetado, "_baixar_planilha", caminho.read_bytes)

    resultado = yd.ipca.taxa_projetada()

    assert resultado.ultima_atualizacao == dt.datetime(2026, 10, 21, 16, 57)
    assert resultado.periodo_referencia == "out/26"
    assert isinstance(resultado.valor_projetado, Decimal)
    assert resultado.valor_projetado.as_tuple() == Decimal("0.0027").as_tuple()
