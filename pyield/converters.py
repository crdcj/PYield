import datetime as dt
from typing import overload

import polars as pl

from pyield.types import ArrayLike, DateLike


def validate_date_format(date_str: str) -> str:
    """Valida se a string de data está em formatos suportados e retorna o *formato*.

    Formatos aceitos:
    - Brasileiro: ``dd-mm-YYYY`` ou ``dd/mm/YYYY`` (day-first explícito)
    - ISO: ``YYYY-mm-dd``

    Regras / Observações:
    - Não fazemos autodetecção ambígua: ``2024-05-06`` só é válido como ISO.
    - Uma coleção (coluna) não deve misturar estilos.
    - Retorna o padrão para uso explícito em ``pd.to_datetime(..., format=fmt)``.

    Returns:
        str: o padrão ``strftime`` correspondente.

    Raises:
        ValueError: se não corresponder a nenhum dos formatos suportados.
    """
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            dt.datetime.strptime(date_str, fmt)
            return fmt
        except ValueError:
            pass
    raise ValueError(
        f"Invalid date format: '{date_str}'."
        + " Accepted formats: dd-mm-YYYY, dd/mm/YYYY ou YYYY-mm-dd."
    )


@overload
def convert_dates(dates: None) -> None: ...
@overload
def convert_dates(dates: DateLike) -> dt.date: ...
@overload
def convert_dates(dates: ArrayLike) -> pl.Series: ...


def convert_dates(
    dates: DateLike | ArrayLike | None,
) -> dt.date | pl.Series | None:
    """Converte diferentes tipos de entrada (escalares ou coleções) para
    ``datetime.date`` (quando escalar) ou ``polars.Series`` com dtype ``Date``.
    """
    if not hasattr(dates, "__len__") or isinstance(dates, str):
        is_scalar = True
        s = pl.Series(values=[dates])
    else:
        is_scalar = False
        s = pl.Series(values=dates)

    if s.dtype == pl.String:
        # Usa primeiro valor não-nulo para determinar o formato.
        first_str = s.str.strip_chars().replace("", None).drop_nulls().first()
        if first_str:
            fmt = validate_date_format(first_str)
            s = s.str.to_date(format=fmt, strict=False)
        else:
            s = pl.Series(values=[None] * s.len(), dtype=pl.Date)
    else:
        # Para todos os outros dtypes (datetime, date, etc.),
        # o cast nativo do Polars é suficiente e muito rápido.
        s = s.cast(pl.Date)

    if is_scalar:
        return s.first()

    return s
