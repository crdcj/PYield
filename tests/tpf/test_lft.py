from decimal import Decimal

from pyield import lft

CASAS_DECIMAIS = 6


def test_cotacao_retorna_decimal_truncado() -> None:
    resultado = lft.cotacao("24-07-2024", "01-09-2030", Decimal("0.001717"))

    assert resultado == Decimal("0.989645")
    assert resultado.as_tuple().exponent == -CASAS_DECIMAIS


def test_pu_retorna_decimal_truncado() -> None:
    resultado = lft.pu(Decimal("15785.324502"), Decimal("0.999291"))

    assert resultado == Decimal("15774.132706")
    assert resultado.as_tuple().exponent == -CASAS_DECIMAIS


def test_cotacao_e_pu_aceitam_float_sem_alterar_resultado() -> None:
    cotacao = lft.cotacao("24-07-2024", "01-09-2030", 0.001717)

    assert cotacao == Decimal("0.989645")
    assert lft.pu(15785.324502, cotacao) == Decimal("15621.867466")


def test_entradas_nulas_retornam_decimal_nan() -> None:
    assert lft.cotacao(None, "01-09-2030", 0.001717).is_nan()
    assert lft.pu(Decimal("NaN"), Decimal("1")).is_nan()


def test_taxa_aceita_pu_decimal() -> None:
    taxa_esperada = 0.00115966
    pu = lft.pu(
        Decimal("15785.324502"),
        lft.cotacao("24-07-2024", "01-03-2025", Decimal("0.00115966")),
    )

    assert (
        lft.taxa("24-07-2024", "01-03-2025", Decimal("15785.324502"), pu)
        == taxa_esperada
    )
