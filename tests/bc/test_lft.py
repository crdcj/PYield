from decimal import Decimal

import pytest

from pyield.bc import lft

CASAS_DECIMAIS = 6

TEXTO_BCB = """
EMISSAO VENCIMENTO DATA BASE TITULO INDICE
03/02/2021 01/09/2024 01/07/2000 210100 14903,011480
30/03/2022 01/03/2025 01/07/2000 210100 14903,011480
99999999*
"""


def test_vna_retorna_decimal_com_escala_da_fonte(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lft, "_baixar_texto", lambda _: TEXTO_BCB)

    resultado = lft.vna("31-05-2024")

    assert resultado == Decimal("14903.011480")
    assert resultado.as_tuple().exponent == -CASAS_DECIMAIS


def test_vna_nulo_retorna_decimal_nan() -> None:
    assert lft.vna(None).is_nan()
