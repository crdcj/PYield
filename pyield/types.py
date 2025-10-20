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

        # 2. Padrão de tipo para Pandas
        case pd.DataFrame() as df:
            return df.empty
        case pd.Series() as s:
            return s.empty

        # 3. Padrão de tipo para Polars
        case pl.DataFrame() as df:
            return df.is_empty()
        case pl.Series() as s:
            return s.is_empty()

        # 4. Padrão de tipo para NaN
        case float() as f:
            return math.isnan(f)

        # 5. Padrão para coleções vazias conhecidas
        case [] | "" | () | {}:
            return True

        # 6. Caso padrão (catch-all)
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
