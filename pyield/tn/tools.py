from typing import overload

import numpy as np
import polars as pl


@overload
def truncate(values: float, decimal_places: int) -> float: ...
@overload
def truncate(values: int, decimal_places: int) -> float: ...
@overload
def truncate(values: pl.Series, decimal_places: int) -> pl.Series: ...


def truncate(values: float | int | pl.Series, decimal_places: int) -> float | pl.Series:
    """Trunca números (scalar ou ``polars.Series``) em direção a zero.

    Implementação unificada usando apenas operações de ``polars``: escalares
    são embrulhados em uma série temporária e depois desembrulhados.

    Args:
        values: Escalar (int/float) ou ``pl.Series``.
        decimal_places: Casas decimais (>= 0).

    Returns:
        Float se entrada era escalar, ou ``pl.Series`` se entrada era série.
    """
    if decimal_places < 0:
        raise ValueError("decimal_places must be non-negative")

    factor = 10**decimal_places
    truncated_values = np.trunc(values * factor) / factor
    if isinstance(truncated_values, np.floating):
        return float(truncated_values)
    else:
        return pl.Series(truncated_values)


def calculate_present_value(
    cash_flows: pl.Series | list[float],
    rates: pl.Series | list[float],
    periods: pl.Series | list[float],
) -> float:
    df = pl.DataFrame({"cash_flows": cash_flows, "rates": rates, "periods": periods})
    if df.is_empty():
        return 0.0

    return (df["cash_flows"] / (1 + df["rates"]) ** df["periods"]).sum()
