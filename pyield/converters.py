import datetime as dt
from typing import overload

import polars as pl

from pyield import types
from pyield.types import ArrayLike, DateLike


def _validar_formato_data(date_str: str) -> str:
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
        >>> _validar_formato_data("25-12-2024")
        '%d-%m-%Y'
        >>> _validar_formato_data("25/12/2024")
        '%d/%m/%Y'
        >>> _validar_formato_data("2024-12-25")
        '%Y-%m-%d'
        >>> _validar_formato_data("12.25.2024")
        Traceback (most recent call last):
        ...
        ValueError: Formato de data inválido: '12.25.2024'. Formatos aceitos: dd-mm-YYYY, dd/mm/YYYY ou YYYY-mm-dd.
        >>> _validar_formato_data("not-a-date")
        Traceback (most recent call last):
        ...
        ValueError: Formato de data inválido: 'not-a-date'. Formatos aceitos: dd-mm-YYYY, dd/mm/YYYY ou YYYY-mm-dd.
    """  # noqa:E501
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            dt.datetime.strptime(date_str, fmt)
            return fmt
        except ValueError:
            pass
    raise ValueError(
        f"Formato de data inválido: '{date_str}'."
        " Formatos aceitos: dd-mm-YYYY, dd/mm/YYYY ou YYYY-mm-dd."
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
    da coleção usando ``_validar_formato_data()``.

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

        Conversão de string ISO escalar:
        >>> convert_dates("2024-12-25")
        datetime.date(2024, 12, 25)

        Conversão de objeto date (passthrough):
        >>> convert_dates(dt.date(2024, 12, 25))
        datetime.date(2024, 12, 25)

        Conversão de datetime para date:
        >>> convert_dates(dt.datetime(2024, 12, 25, 14, 30))
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

        String vazia retorna None:
        >>> convert_dates("") is None
        True

        Lista com valores nulos propaga os nulls:
        >>> convert_dates(["01-01-2024", None])
        shape: (2,)
        Series: '' [date]
        [
            2024-01-01
            null
        ]

        Lista com strings vazias vira série nula:
        >>> convert_dates(["", "  "])
        shape: (2,)
        Series: '' [date]
        [
            null
            null
        ]

        Formatos mistos: formato do primeiro valor válido prevalece:
        >>> convert_dates(["25-12-2024", "2024-12-26"])
        shape: (2,)
        Series: '' [date]
        [
            2024-12-25
            null
        ]
    """
    if not types.is_collection(dates):
        eh_escalar = True
        serie = pl.Series(values=[dates])
    else:
        eh_escalar = False
        serie = pl.Series(values=dates)

    if serie.dtype == pl.String:
        # Usa primeiro valor não-nulo para determinar o formato.
        valores_validos = serie.str.strip_chars().replace("", None).drop_nulls()
        if valores_validos.len() > 0:
            formato = _validar_formato_data(valores_validos.item(0))
            serie = serie.str.to_date(format=formato, strict=False)
        else:
            serie = pl.Series(values=[None] * serie.len(), dtype=pl.Date)
    else:
        # Para todos os outros dtypes (datetime, date, etc.),
        # o cast nativo do Polars é suficiente e muito rápido.
        serie = serie.cast(pl.Date)

    if eh_escalar:
        return serie.item()

    return serie
