import datetime as dt
import math
from typing import Any, Sequence, TypeAlias

import numpy as np
import pandas as pd
import polars as pl

DateLike: TypeAlias = str | np.datetime64 | pd.Timestamp | dt.datetime | dt.date
ArrayLike: TypeAlias = Sequence[Any] | pd.Series | pl.Series | np.ndarray


def _has_nullable_arg(arg) -> bool:  # noqa
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

        # 6. Padrão para string
        case str() if not arg:
            return True

        # 7. Padrão para coleções vazias
        case [] | () | {}:
            return True

        # 8. Caso padrão (catch-all)
        case _:
            return False


def has_nullable_args(*args) -> bool:
    """Verifica se algum dos argumentos fornecidos é None, NaN ou uma coleção vazia.

    Args:
        *args: Uma lista variável de argumentos de qualquer tipo.

    Returns:
        bool: True se algum argumento for considerado "nulo", caso contrário False.
    """
    return any(_has_nullable_arg(arg) for arg in args)


def is_array_like(arg) -> bool:
    if hasattr(arg, "__len__") and not isinstance(arg, (str, bytes)):
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
