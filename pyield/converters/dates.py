import datetime as dt
from typing import overload

import numpy as np
import pandas as pd
import polars as pl

from pyield.types import DateArray, DateScalar


def validate_date_format(date_str) -> None:
    """Validate if the date string is in the correct format (day first)."""
    # Primeiro tenta com hífen (-)
    for fmt in ["%d-%m-%Y", "%d/%m/%Y"]:
        try:
            dt.datetime.strptime(date_str, fmt)
            return
        except ValueError:
            continue
    raise ValueError(
        f"Invalid format: {date_str}. Day first is required (e.g. '31-05-2024')."
    )


@overload
def convert_dates(dates: DateScalar) -> dt.date | None: ...
@overload
def convert_dates(dates: DateArray) -> pl.Series: ...


def convert_dates(  # noqa
    dates: DateScalar | DateArray,
) -> dt.date | pl.Series | None:
    """
    Converte diferentes tipos de entrada (escalares ou coleções)
    para dt.date ou pd.Series[date32[pyarrow]].

    - Strings devem estar no formato dd-mm-YYYY ou dd/mm/YYYY.
    - Nulos escalares retornam None.
    - Arrays vazios não são permitidos.
    """
    # Capturar apenas escalares nulos: a verificação `is True` é crucial,
    # pois `pd.isna()` retorna um array booleano para entradas de array,
    # e um array nulo é idêntico ao objeto singleton `True`.
    if pd.isna(dates) is True:
        return None

    # --- LÓGICA ESCALAR (simples e rápida) ---
    match dates:
        case str():
            validate_date_format(dates)
            return pd.to_datetime(dates, dayfirst=True).date()
        case dt.datetime() | pd.Timestamp():
            return dates.date()
        case dt.date():
            return dates
        case np.datetime64():
            return pd.to_datetime(dates).date()

    # --- LÓGICA DE ARRAY (adaptada para Polars) ---
    # Converte a entrada para uma Série Polars imediatamente.
    # O construtor do Polars é muito bom em lidar com vários tipos de entrada.
    s = pl.Series(dates)

    if s.is_empty():
        raise ValueError("'dates' cannot be an empty Array.")

    # Se a série contiver strings, usamos a validação e o poder de parsing do Pandas,
    if s.dtype == pl.String:
        # Valida o formato usando o primeiro elemento não nulo (mesma lógica de antes)
        first_str = s.drop_nulls().first()
        if first_str:
            validate_date_format(first_str)

        pd_series_for_parsing = s.to_pandas(use_pyarrow_extension_array=True)
        parsed_dates = pd.to_datetime(pd_series_for_parsing, dayfirst=True).dt.date
        return pl.Series(parsed_dates, dtype=pl.Date)

    # Para todos os outros dtypes (numéricos, datetime, date, etc.),
    # o cast nativo do Polars é suficiente e muito rápido.
    return s.cast(pl.Date)


def to_numpy_date_type(
    dates: pd.Timestamp | pd.Series,
) -> np.datetime64 | np.ndarray:
    """
    Converts the input dates to a numpy datetime64[D] format.

    Args:
        dates (Timestamp | Series): A single date or a Series of dates.

    Returns:
        np.datetime64 | np.ndarray: The input dates in a numpy datetime64[D] format.
    """
    if pd.isna(dates) is True:
        return np.datetime64("NaT")

    # # cobre datetime.date, datetime.datetime e pd.Timestamp
    if isinstance(dates, dt.date):
        return np.datetime64(dates, "D")

    if isinstance(dates, pd.Series):
        return dates.to_numpy().astype("datetime64[D]")

    raise ValueError("Invalid input type for 'dates'.")
