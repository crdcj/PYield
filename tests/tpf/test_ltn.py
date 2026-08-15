from decimal import Decimal

from pyield import ltn


def test_pu_retorna_decimal_e_preserva_escala() -> None:
    taxa = Decimal("0.12145")
    esperado = Decimal("535.279902")
    resultado = ltn.pu("05-07-2024", "01-01-2030", taxa)

    assert resultado.as_tuple() == esperado.as_tuple()
    assert ltn.taxa("05-07-2024", "01-01-2030", resultado) == float(taxa)
