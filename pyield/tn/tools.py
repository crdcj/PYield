from typing import overload

import polars as pl

from pyield.types import ArrayLike, is_array_like


@overload
def truncate(values: float, decimal_places: int) -> float: ...
@overload
def truncate(values: ArrayLike, decimal_places: int) -> pl.Series: ...


def truncate(values: float | ArrayLike, decimals: int) -> float | pl.Series:
    """Trunca números (scalar ou ``polars.Series``) em direção a zero.

    Implementação unificada usando apenas operações de ``polars``: escalares
    são embrulhados em uma série temporária e depois desembrulhados.

    Args:
        values: Escalar (int/float) ou ``pl.Series``.
        decimal_places: Casas decimais (>= 0).

    Returns:
        Float se entrada era escalar, ou ``pl.Series`` se entrada era série.

    Examples:
        >>> truncate(3.14159, 3)
        3.141
        >>> truncate(pl.Series([3.14159, 2.71828]), 3)
        shape: (2,)
        Series: '' [f64]
        [
           3.141
           2.718
        ]

    """
    if values is None:
        return float("nan")

    if decimals < 0:
        raise ValueError("decimal_places must be non-negative")

    factor = 10.0**decimals

    if not is_array_like(values):
        return int(values * factor) / factor

    return (values * factor).cast(pl.Int64).cast(pl.Float64) / factor


def calculate_present_value(
    cash_flows: pl.Series | list[float],
    rates: pl.Series | list[float],
    periods: pl.Series | list[float],
) -> float:
    """Calcula o valor presente de uma série de fluxos de caixa de forma estrita.

    O valor presente é calculado descontando cada fluxo de caixa pela taxa
    correspondente e período, usando a fórmula: VP = CF / (1 + r)^t

    Args:
        cash_flows: Fluxos de caixa a descontar.
        rates: Taxas de desconto para cada fluxo (em decimal, ex: 0.10 para 10%).
        periods: Períodos (em anos) para cada fluxo de caixa.

    Returns:
        Soma dos valores presentes. Retorna ``float('nan')`` se:
        - As listas/séries de entrada estiverem vazias.
        - As listas/séries de entrada tiverem tamanhos diferentes.
        - Qualquer valor de entrada ou resultado do cálculo for null, NaN ou inf.

    Examples:
        Título com cupons anuais de 10% e principal de R$1000, descontado a 8% a.a.:
        >>> cash_flows = [100, 100, 1100]  # Cupons de R$100 + principal no vencimento
        >>> rates = [0.08, 0.08, 0.08]  # Taxa de desconto de 8% a.a.
        >>> periods = [1.0, 2.0, 3.0]  # Pagamentos anuais
        >>> round(calculate_present_value(cash_flows, rates, periods), 2)
        1051.54

        Retorna NaN para entradas vazias:
        >>> import math
        >>> math.isnan(calculate_present_value([], [], []))
        True

        Retorna NaN para tamanhos incompatíveis:
        >>> math.isnan(calculate_present_value([100], [0.10, 0.10], [1.0]))
        True
    """
    try:
        # A criação do DataFrame agora pode levantar um ShapeError
        df = pl.DataFrame(
            {
                "cash_flows": cash_flows,
                "rates": rates,
                "periods": periods,
            },
            schema={
                "cash_flows": pl.Float64,
                "rates": pl.Float64,
                "periods": pl.Float64,
            },
        )
    except pl.exceptions.ShapeError:
        return float("nan")

    if df.is_empty():
        return float("nan")

    present_values_series = df["cash_flows"] / (1 + df["rates"]) ** df["periods"]

    if present_values_series.has_nulls():
        return float("nan")

    return present_values_series.sum()
