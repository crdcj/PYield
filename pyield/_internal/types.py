import datetime as dt
import math
from collections.abc import Sized
from typing import Any, TypeAlias, TypeGuard

import polars as pl

DateLike: TypeAlias = str | dt.datetime | dt.date
ArrayLike: TypeAlias = list[Any] | tuple[Any, ...] | pl.Series
DatesLike: TypeAlias = list[DateLike] | tuple[DateLike, ...] | pl.Series


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


def is_array_like(arg: Any) -> TypeGuard[ArrayLike]:
    """Verifica se o argumento segue o contrato interno de ``ArrayLike``."""
    return isinstance(arg, (list, tuple, pl.Series))


def any_is_array_like(*args: Any) -> bool:
    """Verifica se algum dos argumentos fornecidos é ``ArrayLike``.

    Args:
        *args: Uma lista variável de argumentos de qualquer tipo.

    Returns:
        bool: True se algum argumento for ``ArrayLike``, caso contrário False.
    """
    return any(is_array_like(arg) for arg in args)
