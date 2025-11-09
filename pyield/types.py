import datetime as dt
import math
from typing import Any, Sequence, TypeAlias

import numpy as np
import pandas as pd
import polars as pl

DateLike: TypeAlias = str | np.datetime64 | pd.Timestamp | dt.datetime | dt.date
ArrayLike: TypeAlias = Sequence[Any] | pd.Series | pl.Series | np.ndarray


def _has_nullable_scalar(arg) -> bool:
    match arg:
        case None:
            return True
        case "":
            return True
        case float() as f:
            return math.isnan(f)
        case _:
            return False


def _has_nullable_array(arg) -> bool:
    match arg:
        case pd.DataFrame() | pd.Series() | pd.Index() as pd_obj:
            return pd_obj.empty
        case pl.DataFrame() | pl.Series() as pl_obj:
            return pl_obj.is_empty()
        case np.ndarray() as arr:
            return arr.size == 0
        case [] | () | {}:
            return True
        case _:
            return False


def has_nullable_array_args(*args) -> bool:
    """Verifica se algum dos argumentos fornecidos é None, NaN ou uma coleção vazia.

    Args:
        *args: Uma lista variável de argumentos de qualquer tipo.

    Returns:
        bool: True se algum argumento for considerado "nulo", caso contrário False.
    """
    return any(_has_nullable_array(arg) for arg in args)


def has_nullable_scalar_args(*args) -> bool:
    """Verifica se algum dos argumentos fornecidos é None, NaN ou uma coleção vazia.

    Args:
        *args: Uma lista variável de argumentos de qualquer tipo.

    Returns:
        bool: True se algum argumento for considerado "nulo", caso contrário False.
    """
    return any(_has_nullable_scalar(arg) for arg in args)


def _has_null_arg(arg) -> bool:  # noqa
    match arg:
        # 1. Singletons
        case None:
            return True

        # 2. Padrão de tipo para Pandas (expandido para incluir pd.Index)
        case pd.DataFrame() | pd.Series() | pd.Index() as pd_obj:
            return pd_obj.empty

        # 3. Padrão de tipo para Polars
        case pl.DataFrame() | pl.Series() as pl_obj:
            return pl_obj.is_empty()

        # 4. Padrão de tipo para NumPy (adicionado)
        case np.ndarray() as arr:
            return arr.size == 0

        # 5. Padrão de tipo para NaN
        case float() as f:
            return math.isnan(f)

        # 6. Padrão para coleções vazias
        case [] | "" | () | {}:
            return True

        # 7. Caso padrão (catch-all)
        case _:
            return False


def has_null_args(*args) -> bool:
    """Verifica se algum dos argumentos fornecidos é None, NaN ou uma coleção vazia.

    Args:
        *args: Uma lista variável de argumentos de qualquer tipo.

    Returns:
        bool: True se algum argumento for considerado "nulo", caso contrário False.
    """
    return any(_has_null_arg(arg) for arg in args)


def is_array_like(arg) -> bool:
    if hasattr(arg, "__len__") and not isinstance(arg, str):
        return True
    return False


def has_array_like_args(*args) -> bool:
    """Verifica se algum dos argumentos fornecidos é uma coleção (array-like).

    Args:
        *args: Uma lista variável de argumentos de qualquer tipo.

    Returns:
        bool: True se algum argumento for uma coleção, caso contrário False.
    """
    return any(is_array_like(arg) for arg in args)
