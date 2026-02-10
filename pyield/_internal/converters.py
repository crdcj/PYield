import datetime as dt
from typing import overload

import polars as pl

from pyield._internal import types
from pyield._internal.types import ArrayLike, DateLike


def converter_datas_expr(expr: pl.Expr | str) -> pl.Expr:
    """Converte expressão Polars para ``Date`` com parse tolerante por linha.

    Esta função é voltada para pipelines em ``DataFrame``/``LazyFrame``.
    O parse aceita os formatos ``dd-mm-YYYY``, ``dd/mm/YYYY`` e ``YYYY-mm-dd``.
    Valores inválidos (incluindo string vazia) viram ``null``.

    Args:
        expr: Expressão Polars ou nome da coluna com datas.

    Returns:
        Uma ``pl.Expr`` com dtype ``Date``.

    Examples:
        >>> import polars as pl
        >>> df = pl.DataFrame(
        ...     {"d": ["02-01-2024", "03/01/2024", "2024-01-04", "31-02-2024", ""]}
        ... )
        >>> df.select(converter_datas_expr("d"))
        shape: (5, 1)
        ┌────────────┐
        │ d          │
        │ ---        │
        │ date       │
        ╞════════════╡
        │ 2024-01-02 │
        │ 2024-01-03 │
        │ 2024-01-04 │
        │ null       │
        │ null       │
        └────────────┘
    """
    if isinstance(expr, str):
        expr = pl.col(expr)

    # Fallback por linha: o primeiro valor não-nulo vence.
    # Se o elemento já for Date, o cast resolve no 1o termo.
    # Se o elemento for null, ele permanece null em todos os termos.
    expr_str = expr.cast(pl.String, strict=False).str.strip_chars()
    return pl.coalesce(
        expr.cast(pl.Date, strict=False),
        expr_str.str.to_date(format="%d-%m-%Y", strict=False),
        expr_str.str.to_date(format="%d/%m/%Y", strict=False),
        expr_str.str.to_date(format="%Y-%m-%d", strict=False),
    )


@overload
def converter_datas(dates: None) -> None: ...
@overload
def converter_datas(dates: DateLike) -> dt.date: ...
@overload
def converter_datas(dates: ArrayLike) -> pl.Series: ...


def converter_datas(
    dates: DateLike | ArrayLike | None,
) -> dt.date | pl.Series | None:
    """Converte diferentes tipos de entrada para ``datetime.date`` ou ``pl.Series``.

    Normaliza datas em diversos formatos para um tipo consistente:
    - Entradas escalares (str, date, datetime) retornam ``datetime.date``
    - Coleções (list, tuple, ndarray, Series) retornam ``pl.Series`` com dtype ``Date``
    - Entradas nulas retornam ``None``

    O parse de strings é feito por elemento com fallback entre formatos
    suportados. Valores inválidos viram ``null``.

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
        >>> converter_datas("25-12-2024")
        datetime.date(2024, 12, 25)

        Conversão de string ISO escalar:
        >>> converter_datas("2024-12-25")
        datetime.date(2024, 12, 25)

        Conversão de objeto date (passthrough):
        >>> converter_datas(dt.date(2024, 12, 25))
        datetime.date(2024, 12, 25)

        Conversão de datetime para date:
        >>> converter_datas(dt.datetime(2024, 12, 25, 14, 30))
        datetime.date(2024, 12, 25)

        Conversão de lista de strings:
        >>> converter_datas(["01-01-2024", "15-06-2024"])
        shape: (2,)
        Series: 'dates' [date]
        [
            2024-01-01
            2024-06-15
        ]

        Entrada nula retorna None:
        >>> converter_datas(None) is None
        True

        String vazia retorna None:
        >>> converter_datas("") is None
        True

        Lista com valores nulos propaga os nulls:
        >>> converter_datas(["01-01-2024", None])
        shape: (2,)
        Series: 'dates' [date]
        [
            2024-01-01
            null
        ]

        Lista com strings vazias vira série nula:
        >>> converter_datas(["", "  "])
        shape: (2,)
        Series: 'dates' [date]
        [
            null
            null
        ]

        Formatos mistos por linha:
        >>> converter_datas(["25-12-2024", "2024-12-26"])
        shape: (2,)
        Series: 'dates' [date]
        [
            2024-12-25
            2024-12-26
        ]
    """
    if not types.is_collection(dates):
        eh_escalar = True
        serie = pl.Series(values=[dates])
    else:
        eh_escalar = False
        serie = pl.Series(values=dates)

    serie = (
        pl.DataFrame({"dates": serie}, nan_to_null=True)
        .select(converter_datas_expr("dates"))
        .get_column("dates")
    )

    if eh_escalar:
        return serie.item()

    return serie
