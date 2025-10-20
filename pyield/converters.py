import datetime as dt
from typing import overload

import pandas as pd
import polars as pl

from pyield.types import DateArray, DateScalar, has_null_args


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
def convert_dates(dates: DateScalar) -> dt.date | None: ...
@overload
def convert_dates(dates: DateArray) -> pl.Series: ...
def convert_dates(  # noqa
    dates: DateScalar | DateArray,
) -> dt.date | pl.Series | None:
    """Converte diferentes tipos de entrada (escalares ou coleções) para
    ``datetime.date`` (quando escalar) ou ``polars.Series`` com dtype ``Date``.

    Regras:
        - Strings devem estar em um dos formatos suportados:
            ``dd-mm-YYYY``, ``dd/mm/YYYY`` ou ``YYYY-mm-dd``.
    - Valor escalar nulo (``None``, ``NaT``) retorna ``None``.
    - Arrays vazios não são permitidos (levanta ``ValueError``).
        - Coleções de strings não podem misturar formatos; o primeiro valor
            não-nulo define o formato usado para toda a série.
    """
    if has_null_args(dates):
        return None

    # --- LÓGICA ESCALAR (simples e rápida) ---
    match dates:
        case str():
            fmt = validate_date_format(dates)
            return pd.to_datetime(dates, format=fmt).date()
        case dt.datetime() | pd.Timestamp():
            return dates.date()
        case dt.date():
            return dates
        # Removido suporte explícito a np.datetime64 (não usado mais);
        # se aparecer em coleções, Polars lidará no caminho de array.

    # --- LÓGICA DE ARRAY (adaptada para Polars) ---
    # Converte a entrada para uma Série Polars imediatamente.
    # O construtor do Polars é muito bom em lidar com vários tipos de entrada.
    s = pl.Series(dates)

    if s.is_empty():
        raise ValueError("'dates' cannot be an empty Array.")

    # Se a série contiver strings, usamos a validação e o poder de parsing do Pandas,
    if s.dtype == pl.String:
        # Usa primeiro valor não-nulo para determinar o formato.
        first_str = s.drop_nulls().first()
        if first_str is None:
            # Série só com nulls: retorna série vazia (nulls) como Date.
            return s.cast(pl.Date)
        fmt = validate_date_format(first_str)

        pd_series_for_parsing = s.to_pandas(use_pyarrow_extension_array=True)
        # Parsing explícito usando o formato detectado; erros levantam exception.
        parsed_dates = pd.to_datetime(pd_series_for_parsing, format=fmt).dt.date
        return pl.Series(parsed_dates, dtype=pl.Date)

    # Para todos os outros dtypes (datetime, date, etc.),
    # o cast nativo do Polars é suficiente e muito rápido.
    return s.cast(pl.Date)
