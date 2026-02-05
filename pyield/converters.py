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
    - Retorna o padrão para uso explícito em ``pl.Series.str.to_date(..., format=fmt)``.

    Args:
        date_str: String de data a ser validada.

    Returns:
        O padrão ``strftime`` correspondente ao formato detectado.

    Raises:
        ValueError: Se não corresponder a nenhum dos formatos suportados.

    Examples:
        >>> validate_date_format("25-12-2024")
        '%d-%m-%Y'

        >>> validate_date_format("25/12/2024")
        '%d/%m/%Y'

        >>> validate_date_format("2024-12-25")
        '%Y-%m-%d'
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
    """Converte diferentes tipos de entrada para ``datetime.date`` ou ``pl.Series``.

    Normaliza datas em diversos formatos para um tipo consistente:
    - Entradas escalares (str, date, datetime) retornam ``datetime.date``
    - Coleções (list, tuple, ndarray, Series) retornam ``pl.Series`` com dtype ``Date``
    - Entradas nulas retornam ``None``

    O formato da string é detectado automaticamente pelo primeiro valor não-nulo
    da coleção usando ``validate_date_format()``.

    Args:
        dates: Data(s) a converter. Aceita:
            - String nos formatos ``dd-mm-YYYY``, ``dd/mm/YYYY`` ou ``YYYY-mm-dd``
            - ``datetime.date`` ou ``datetime.datetime``
            - Lista, tupla, ndarray ou Series de datas
            - ``None``

    Returns:
        ``datetime.date`` para entrada escalar, ``pl.Series`` para coleções,
        ou ``None`` para entrada nula.

    Examples:
        Conversão de string escalar:
        >>> import datetime as dt
        >>> convert_dates("25-12-2024")
        datetime.date(2024, 12, 25)

        Conversão de objeto date (passthrough):
        >>> convert_dates(dt.date(2024, 12, 25))
        datetime.date(2024, 12, 25)

        Conversão de lista de strings:
        >>> convert_dates(["01-01-2024", "15-06-2024"])
        shape: (2,)
        Series: '' [date]
        [
            2024-01-01
            2024-06-15
        ]

        Entrada nula retorna None:
        >>> convert_dates(None) is None
        True

        Lista com valores nulos propaga os nulls:
        >>> convert_dates(["01-01-2024", None])
        shape: (2,)
        Series: '' [date]
        [
            2024-01-01
            null
        ]
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
