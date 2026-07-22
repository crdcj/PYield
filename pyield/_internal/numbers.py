from decimal import Decimal
from typing import overload

import polars as pl


@overload
def truncar(values: float | int | Decimal, decimals: int) -> float: ...


@overload
def truncar(values: pl.Series, decimals: int) -> pl.Series: ...


def truncar(
    values: float | int | Decimal | pl.Series, decimals: int
) -> float | pl.Series:
    """Trunca floats escalares ou séries em direção a zero.

    Args:
        values: Número escalar ou série Polars.
        decimals: Quantidade de casas decimais, maior ou igual a zero.

    Returns:
        Float para entrada escalar ou ``pl.Series`` para entrada em série.

    Examples:
        >>> truncar(3.14159, 3)
        3.141
        >>> truncar(float("nan"), 3)
        nan
        >>> truncar(pl.Series([3.14159, 2.71828]), 3)
        shape: (2,)
        Series: '' [f64]
        [
           3.141
           2.718
        ]
    """
    if decimals < 0:
        raise ValueError("decimals must be non-negative")

    if isinstance(values, pl.Series):
        return values.truncate(decimals)
    return pl.Series([float(values)]).truncate(decimals).item()
