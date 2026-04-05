"""Taxa DI over da B3/CETIP (servidor FTP).

Fonte:
    ftp://ftp.cetip.com.br/MediaCDI/

Formato do arquivo (ex.: 20250228.txt):
    00001315

Notas de implementação:
    - O valor "00001315" representa 1315 / 10^4 = 0.1315 (13,15% a.a.).
    - Arquivos ausentes (feriados/fins de semana) retornam erro FTP 550.
"""

import ftplib
import logging

from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas
from pyield._internal.types import DateLike, any_is_empty

registro = logging.getLogger(__name__)

# 4 casas decimais na taxa = 2 casas decimais em percentual
CASAS_DECIMAIS_DI_OVER = 4


@ttl_cache()
def _buscar_taxa(nome_arquivo: str) -> float:
    """Busca a taxa DI no FTP da CETIP para o arquivo informado."""
    try:
        with ftplib.FTP("ftp.cetip.com.br", timeout=10) as ftp:
            ftp.login()
            ftp.cwd("/MediaCDI")

            linhas = []
            try:
                ftp.retrlines(f"RETR {nome_arquivo}", linhas.append)
            except ftplib.error_perm as e:
                # Código 550 = arquivo não encontrado (feriado/fim de semana)
                if str(e).startswith("550"):
                    return float("nan")
                raise

            if not linhas:
                registro.error("Arquivo %s está vazio.", nome_arquivo)
                return float("nan")

            # Formato usual: "00001315" -> 13.15% -> 0.1315
            taxa_bruta = linhas[0].strip()
            taxa = int(taxa_bruta) / 10**CASAS_DECIMAIS_DI_OVER
            return round(taxa, CASAS_DECIMAIS_DI_OVER)

    except ftplib.all_errors as e:
        registro.error("Erro de conexão ou transferência FTP: %s", e)
        raise ConnectionError(f"Falha ao buscar taxa DI via FTP: {e}") from e


def di_over(data: DateLike) -> float:
    """Obtém a taxa DI over via FTP da B3/CETIP.

    Busca o arquivo de taxa DI (Depósito Interfinanceiro) no servidor
    FTP da CETIP para a data informada.

    Args:
        data: data da consulta para buscar a taxa DI.

    Returns:
        Taxa DI para a data especificada (ex: 0.1315 para 13,15%).
        Retorna ``nan`` se a data for feriado ou fim de semana.

    Examples:
        >>> di_over("28/02/2025")
        0.1315
        >>> di_over("01/01/2025")  # Feriado
        nan
    """
    if any_is_empty(data):
        return float("nan")

    data_ref = converter_datas(data)
    nome_arquivo = data_ref.strftime("%Y%m%d.txt")
    return _buscar_taxa(nome_arquivo)
