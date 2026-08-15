import math
from decimal import Decimal

import polars as pl
import pytest

from pyield import ltn


def test_pu_retorna_decimal_e_preserva_escala() -> None:
    taxa = Decimal("0.12145")
    esperado = Decimal("535.279902")
    resultado = ltn.pu("05-07-2024", "01-01-2030", taxa)

    assert resultado.as_tuple() == esperado.as_tuple()
    assert ltn.taxa("05-07-2024", "01-01-2030", resultado) == float(taxa)


@pytest.mark.parametrize("data_liquidacao", ["01-01-2027", "05-01-2027"])
def test_calculos_rejeitam_prazo_nao_positivo(data_liquidacao: str) -> None:
    data_vencimento = "01-01-2027"

    assert ltn.pu(data_liquidacao, data_vencimento, 0.10).is_nan()
    assert math.isnan(ltn.taxa(data_liquidacao, data_vencimento, 900))
    assert math.isnan(ltn.dv01(data_liquidacao, data_vencimento, 0.10, 1_000))


@pytest.mark.parametrize("data_liquidacao", ["01-01-2027", "05-01-2027"])
def test_expressoes_rejeitam_prazo_nao_positivo(data_liquidacao: str) -> None:
    df = pl.DataFrame(
        {
            "data_liquidacao": [data_liquidacao],
            "data_vencimento": ["01-01-2027"],
            "taxa": [0.10],
            "pu": [1_000.0],
        }
    ).select(
        duration=ltn.duration_expr("data_liquidacao", "data_vencimento"),
        dv01=ltn.dv01_expr(
            "data_liquidacao",
            "data_vencimento",
            "taxa",
            "pu",
        ),
    )

    assert math.isnan(df["duration"].item())
    assert math.isnan(df["dv01"].item())
