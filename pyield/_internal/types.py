import datetime as dt
import math
from collections.abc import Collection, Sized
from typing import Any, Sequence, TypeAlias

import polars as pl

DateLike: TypeAlias = str | dt.datetime | dt.date
ArrayLike: TypeAlias = Sequence[Any] | pl.Series


def _is_empty(arg: Any) -> bool:
    """Verifica se um argumento é None, NaN ou uma coleção vazia."""
    if arg is None:
        return True
    if isinstance(arg, float):
        return math.isnan(arg)
    if isinstance(arg, Sized):
        return len(arg) == 0
    return False


def any_is_empty(*args: Any) -> bool:
    """Verifica se algum dos argumentos fornecidos é None, NaN ou uma coleção vazia.

    Args:
        *args: Uma lista variável de argumentos de qualquer tipo.

    Returns:
        bool: True se algum argumento for considerado "nulo", caso contrário False.
    """
    return any(_is_empty(arg) for arg in args)


def is_collection(arg: Any) -> bool:
    """Verifica se o argumento é uma coleção (array-like), excluindo strings e bytes."""
    return isinstance(arg, Collection) and not isinstance(arg, (str, bytes))


def any_is_collection(*args: Any) -> bool:
    """Verifica se algum dos argumentos fornecidos é uma coleção (array-like).

    Args:
        *args: Uma lista variável de argumentos de qualquer tipo.

    Returns:
        bool: True se algum argumento for uma coleção, caso contrário False.
    """
    return any(is_collection(arg) for arg in args)
