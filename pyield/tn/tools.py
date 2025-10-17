from typing import overload

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
    is_scalar = isinstance(values, (int, float))

    series = pl.Series([values]) if is_scalar else values
    # Multiplica, trunca via cast para inteiro e divide para voltar à escala
    truncated = (series * factor).cast(pl.Int64) / factor
    truncated = truncated.cast(pl.Float64)

    if is_scalar:
        # Retorna primeiro elemento como float
        return truncated.item()
    return truncated


def calculate_present_value(
    cash_flows: pl.Series,
    rates: pl.Series,
    periods: pl.Series,
) -> float:
    if cash_flows.is_empty() or rates.is_empty() or periods.is_empty():
        return 0  # Return 0 if any input is empty

    # Check if data have the same length
    if len(cash_flows) != len(rates) or len(cash_flows) != len(periods):
        raise ValueError("All series must have the same length.")

    return (cash_flows / (1 + rates) ** periods).sum()
