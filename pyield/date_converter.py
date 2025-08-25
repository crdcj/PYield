import datetime as dt
from typing import overload

import numpy as np
import pandas as pd

DateScalar = str | np.datetime64 | pd.Timestamp | dt.datetime | dt.date
DateArray = (
    pd.Series | pd.DatetimeIndex | np.ndarray | list[DateScalar] | tuple[DateScalar]
)


def validate_date_format(date_str):
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
def convert_input_dates(dates: DateScalar) -> pd.Timestamp: ...
@overload
def convert_input_dates(dates: DateArray) -> pd.Series: ...


def convert_input_dates(
    dates: DateScalar | DateArray,
) -> pd.Timestamp | pd.Series:
    # Capturar apenas escalares nulos: a verificação `is True` é crucial,
    # pois `pd.isna()` retorna um array booleano para entradas de array,
    # e um array nunca é idêntico ao objeto singleton `True`.
    if pd.isna(dates) is True:
        return pd.NaT

    match dates:
        case str():
            validate_date_format(dates)
            return pd.to_datetime(dates, dayfirst=True)

        case dt.date():
            return pd.Timestamp(dates)

        case dt.datetime() | np.datetime64() | pd.Timestamp():
            return pd.Timestamp(dates).normalize()

        case pd.Series() | np.ndarray() | list() | tuple():
            # Preserve input values making a copy
            result = pd.Series(dates)

            if result.empty:
                raise ValueError("'dates' cannot be an empty Array.")

            if pd.api.types.is_string_dtype(result):
                # Check first element to validate date format
                validate_date_format(result.iloc[0])

            return pd.to_datetime(result, dayfirst=True).astype("datetime64[ns]")

        case pd.DatetimeIndex():
            return pd.Series(dates).astype("datetime64[ns]")

        case _:
            raise ValueError("Invalid input type for 'dates'.")


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
    elif isinstance(dates, pd.Timestamp) or dates is pd.NaT:
        return np.datetime64(dates, "D")
    elif isinstance(dates, pd.Series):
        return dates.to_numpy().astype("datetime64[D]")
    else:
        raise ValueError("Invalid input type for 'dates'.")
