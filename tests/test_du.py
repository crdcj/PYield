import datetime as dt

import polars as pl
import pytest

from pyield import du

DATA_ZUMBI = dt.date(2024, 11, 20)
DIA_SEGUINTE = dt.date(2024, 11, 21)


@pytest.mark.parametrize(
    ("calendario", "contagem", "data_deslocada", "dia_util"),
    [
        ("auto", 0, DIA_SEGUINTE, False),
        ("anterior", 1, DATA_ZUMBI, True),
        ("atual", 0, DIA_SEGUINTE, False),
    ],
)
def test_calendario_funcoes_escalares(
    calendario: str,
    contagem: int,
    data_deslocada: dt.date,
    dia_util: bool,
) -> None:
    assert du.contar(DATA_ZUMBI, DIA_SEGUINTE, calendario) == contagem
    assert du.deslocar(DATA_ZUMBI, 0, "forward", calendario) == data_deslocada
    assert du.eh_dia_util(DATA_ZUMBI, calendario) is dia_util


@pytest.mark.parametrize(
    ("calendario", "esperado"),
    [
        ("auto", (0, DIA_SEGUINTE, False)),
        ("anterior", (1, DATA_ZUMBI, True)),
        ("atual", (0, DIA_SEGUINTE, False)),
    ],
)
def test_calendario_expressoes(
    calendario: str,
    esperado: tuple[int, dt.date, bool],
) -> None:
    df = pl.DataFrame({"inicio": [DATA_ZUMBI], "fim": [DIA_SEGUINTE]})

    resultado = df.select(
        contagem=du.contar_expr("inicio", "fim", calendario),
        data_deslocada=du.deslocar_expr("inicio", 0, "forward", calendario),
        dia_util=du.eh_dia_util_expr("inicio", calendario),
    )

    assert resultado.row(0) == esperado


@pytest.mark.parametrize(
    ("calendario", "datas_esperadas"),
    [
        ("auto", []),
        ("anterior", [DATA_ZUMBI]),
        ("atual", []),
    ],
)
def test_calendario_gerar(
    calendario: str,
    datas_esperadas: list[dt.date],
) -> None:
    resultado = du.gerar(DATA_ZUMBI, DATA_ZUMBI, calendario=calendario)

    assert resultado.to_list() == datas_esperadas
