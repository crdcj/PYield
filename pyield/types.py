import datetime as dt
import math

import numpy as np
import pandas as pd
import polars as pl

DateScalar = str | np.datetime64 | pd.Timestamp | dt.datetime | dt.date
DateArray = (
    pd.DatetimeIndex
    | pd.Series
    | pl.Series
    | np.ndarray
    | list[DateScalar]
    | tuple[DateScalar, ...]
)
FloatArray = np.ndarray | list[float] | tuple[float, ...] | pd.Series | pl.Series
IntegerScalar = int | np.integer
IntegerArray = np.ndarray | list | tuple | pl.Series | pd.Series


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

        # 6. Padrão para coleções vazias conhecidas (agora seguro)
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
