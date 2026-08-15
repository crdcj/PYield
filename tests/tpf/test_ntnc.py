from decimal import Decimal

from pyield import ntnc

CASAS_DECIMAIS = 6
TAXA_ESPERADA = 0.06762593


def test_cotacao_e_pu_retornam_decimal() -> None:
    cotacao = ntnc.cotacao("21-03-2025", "01-01-2031", Decimal("0.067626"))

    assert cotacao == Decimal("1.264958")
    assert cotacao.as_tuple().exponent == -CASAS_DECIMAIS
    assert ntnc.pu(Decimal("6598.913723"), cotacao) == Decimal("8347.348705")


def test_cotacao_e_pu_nulos_retornam_decimal_nan() -> None:
    assert ntnc.cotacao(None, "01-01-2031", Decimal("0.067626")).is_nan()
    assert ntnc.pu(Decimal("NaN"), Decimal("1.264958")).is_nan()


def test_taxa_aceita_pu_decimal() -> None:
    pu = ntnc.pu(
        Decimal("6598.913723"),
        ntnc.cotacao("21-03-2025", "01-01-2031", Decimal("0.067626")),
    )

    assert (
        ntnc.taxa("21-03-2025", "01-01-2031", Decimal("6598.913723"), pu)
        == TAXA_ESPERADA
    )


def test_vna_projetado_retorna_decimal() -> None:
    assert ntnc.vna_projetado(
        "16-06-2026", Decimal("6693.537239"), Decimal("0.30")
    ) == Decimal("6703.570025")
