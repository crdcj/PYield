import datetime as dt
import inspect

import pytest

from pyield import ntnf


def test_fluxos_caixa_preservam_data_contratual() -> None:
    resultado = ntnf.fluxos_caixa("01-08-2024", "01-01-2025")

    assert resultado["data_pagamento"].to_list() == [dt.date(2025, 1, 1)]


def test_fluxos_caixa_nao_expoem_ajuste_de_datas() -> None:
    parametros = inspect.signature(ntnf.fluxos_caixa).parameters

    assert tuple(parametros) == ("data_liquidacao", "data_vencimento")


@pytest.mark.parametrize(
    ("data_vencimento", "taxa", "pu_esperado"),
    [
        ("01-01-2031", 0.145366, 864.701572),
        ("01-01-2033", 0.145764, 823.854014),
        ("01-01-2037", 0.145259, 773.756861),
    ],
)
def test_pu_e_taxa_reproduzem_referencia_do_back_office(
    data_vencimento: str,
    taxa: float,
    pu_esperado: float,
) -> None:
    """Reproduz preços de NTN-F validados pelo back office em 06/07/2026."""
    data_liquidacao = "06-07-2026"

    assert ntnf.pu(data_liquidacao, data_vencimento, taxa) == pu_esperado
    assert ntnf.taxa(data_liquidacao, data_vencimento, pu_esperado) == taxa
