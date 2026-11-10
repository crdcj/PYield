"""Helpers compartilhados pelos parsers do RMD."""

import datetime as dt
import re

import polars as pl

_MESES_PT = {
    "Jan": 1,
    "Fev": 2,
    "Mar": 3,
    "Abr": 4,
    "Mai": 5,
    "Jun": 6,
    "Jul": 7,
    "Ago": 8,
    "Set": 9,
    "Out": 10,
    "Nov": 11,
    "Dez": 12,
}

_PADRAO_ESPACOS = re.compile(r"\s+")
_LINHA_PERIODOS = 2


def parsear_periodo(periodo: str) -> dt.date | None:
    """Converte string de período para ``datetime.date`` ou ``None``."""
    try:
        mes_str, ano_str = periodo.split("/")
    except ValueError:
        return None

    mes = _MESES_PT.get(mes_str)
    if mes is None:
        return None

    return dt.date(2000 + int(ano_str), mes, 1)


def limpar_rotulo(valor: object) -> str:
    """Remove espaços e notas de rodapé do rótulo lido do Excel."""
    texto = str(valor).replace("¹", "").replace("²", "").strip()
    return _PADRAO_ESPACOS.sub(" ", texto)


def obter_periodos_mensais(df_bruto: pl.DataFrame) -> list[tuple[int, dt.date]]:
    """Extrai pares (índice da coluna de dados, data) dos períodos mensais.

    O índice é relativo às colunas de dados, sem a coluna de rótulos. Células
    vazias são ignoradas sem deslocar os índices das colunas seguintes.
    """
    return [
        (i, data)
        for i, periodo in enumerate(df_bruto.row(_LINHA_PERIODOS)[1:])
        if periodo is not None and (data := parsear_periodo(str(periodo))) is not None
    ]
