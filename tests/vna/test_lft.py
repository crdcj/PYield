from decimal import Decimal

import pytest

from pyield import vna
from pyield.vna import _lft as lft  # noqa: PLC2701

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
    monkeypatch.setattr(lft, "_baixar_texto", lambda _, **kwargs: TEXTO_BCB)

    resultado = vna.valor("LFT", "31-05-2024")

    assert resultado == Decimal("14903.011480")
    assert resultado.as_tuple().exponent == -CASAS_DECIMAIS


def test_vna_nulo_retorna_decimal_nan() -> None:
    assert vna.valor("LFT", None).is_nan()


def test_projetado_lft_registra_mais_de_um_dia_util(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING", logger=lft.__name__):
        resultado = lft.projetado_lft(
            "18-09-2026", "22-09-2026", Decimal("19905.773236"), "13.75%"
        )

    assert "um dia útil" in caplog.text
    assert resultado == Decimal("19926.136961")
