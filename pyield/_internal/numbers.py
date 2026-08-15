from decimal import ROUND_DOWN, Decimal
from typing import overload

import polars as pl


@overload
def truncar(values: float | int | Decimal, decimals: int) -> float: ...


@overload
def truncar(values: pl.Series, decimals: int) -> pl.Series: ...


def truncar_decimal(value: float | int | Decimal, decimals: int) -> Decimal:
    """Trunca um escalar e preserva o resultado como Decimal."""
    if decimals < 0:
        raise ValueError("decimals must be non-negative")

    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal.is_finite():
        return decimal
    quantizer = Decimal(f"1e-{decimals}")
    return decimal.quantize(quantizer, rounding=ROUND_DOWN)


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
    return float(truncar_decimal(values, decimals))
