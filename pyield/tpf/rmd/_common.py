"""Helpers compartilhados pelos parsers do RMD."""

import datetime as dt
import re

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
