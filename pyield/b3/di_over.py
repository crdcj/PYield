"""Taxa DI over da B3/CETIP (servidor FTP).

Fonte:
    ftp://ftp.cetip.com.br/MediaCDI/

Formato do arquivo (ex.: 20250228.txt):
    00001315

Notas de implementação:
    - O valor "00001315" representa 1315 / 10^4 = 0.1315 (13,15% a.a.).
    - Arquivos ausentes (feriados/fins de semana) retornam erro FTP 550.
    - Série disponível a partir de 20/08/2012 (primeiro arquivo no FTP).
"""

import datetime as dt
import urllib.error
import urllib.request

from pyield import du
from pyield._internal.converters import converter_datas
from pyield._internal.types import DateLike, any_is_empty

# Primeiro arquivo disponível no FTP da CETIP
DATA_INICIO = dt.date(2012, 8, 20)

# 4 casas decimais na taxa = 2 casas decimais em percentual
CASAS_DECIMAIS_DI_OVER = 4

_URL_BASE = "ftp://ftp.cetip.com.br/MediaCDI/"


def _buscar_taxa(nome_arquivo: str) -> float:
    """Busca a taxa DI no FTP da CETIP para o arquivo informado."""
    try:
        with urllib.request.urlopen(_URL_BASE + nome_arquivo, timeout=10) as r:
            conteudo = r.read().decode().strip()
        taxa = int(conteudo) / 10**CASAS_DECIMAIS_DI_OVER
        return round(taxa, CASAS_DECIMAIS_DI_OVER)
    except urllib.error.URLError as e:
        # Código 550 = arquivo não encontrado (feriado/fim de semana).
        if "550" in str(e.reason):
            return float("nan")
        raise ConnectionError(f"Falha ao buscar taxa DI via FTP: {e.reason}") from e


def di_over(data: DateLike) -> float:
    """Obtém a taxa DI over na camada técnica da B3/CETIP.

    Busca o arquivo de taxa DI (Depósito Interfinanceiro) no servidor
    FTP da CETIP para a data informada. Use ``pyield.di_over`` na
    API pública principal.

    Args:
        data: data da consulta para buscar a taxa DI.

    Returns:
        Taxa DI em decimal ou ``nan`` se a data for feriado, fim de semana
        ou anterior a 20/08/2012 (início da série no FTP).

    Examples:
        >>> import pyield as yd
        >>> taxa = yd.di_over("28/02/2025")  # decimal (0.1315 = 13,15% a.a.)
        >>> f"{taxa:.2%}"
        '13.15%'
        >>> yd.di_over("01/01/2025")  # Feriado
        nan
    """
    if any_is_empty(data):
        return float("nan")

    data_ref = converter_datas(data)
    if data_ref < DATA_INICIO or not du.eh_dia_util(data_ref):
        return float("nan")

    return _buscar_taxa(data_ref.strftime("%Y%m%d.txt"))
