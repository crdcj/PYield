from decimal import Decimal

from pyield import ntnc


def test_cotacao_e_pu_reproduzem_valores_de_referencia() -> None:
    cotacao = ntnc.cotacao("21-03-2025", "01-01-2031", 0.067626)
    pu = ntnc.pu(6598.913723, cotacao)

    assert cotacao == Decimal("126.4958")
    assert pu == Decimal("8347.348705")


def test_cotacao_e_pu_nulos_retornam_decimal_nan() -> None:
    assert ntnc.cotacao(None, "01-01-2031", 0.067626).is_nan()
    assert ntnc.pu(float("nan"), 126.4958).is_nan()


def test_taxa_aceita_pu_decimal() -> None:
    taxa_esperada = Decimal("0.06762593")
    pu = ntnc.pu(6598.913723, ntnc.cotacao("21-03-2025", "01-01-2031", 0.067626))

    assert ntnc.taxa("21-03-2025", "01-01-2031", 6598.913723, pu) == taxa_esperada


def test_cotacao_trunca_taxa_percentual_excedente() -> None:
    assert ntnc.cotacao("21-05-2008", "01-03-2011", "6.9000009%") == Decimal("99.0981")


def test_pu_trunca_vna_e_cotacao_excedentes() -> None:
    assert ntnc.pu(2126.4737349, 99.09819) == Decimal("2107.295067")
