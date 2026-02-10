import ftplib
import logging

from pyield._internal.converters import converter_datas
from pyield._internal.types import DateLike, any_is_empty

logger = logging.getLogger(__name__)

# 4 casas decimais na taxa = 2 casas decimais em percentual
CASAS_DECIMAIS_DI_OVER = 4


def di_over(date: DateLike) -> float:
    """
    Obtém a taxa DI (Depósito Interfinanceiro) para uma data específica do
    servidor FTP da B3/CETIP.

    Args:
        date (DateLike): Data de referência para buscar a taxa DI.

    Returns:
        float: Taxa DI para a data especificada (ex: 0.1315 para 13.15%).
               Retorna float("nan") se o arquivo não for encontrado (ex: fins
               de semana, feriados).

    Raises:
        ValueError: Se a data não estiver no formato correto.
        ConnectionError: Se a conexão com o servidor FTP falhar ou ocorrerem
            outros erros de transferência.

    Examples:
        >>> di_over("28/02/2025")
        0.1315
        >>> di_over("01/01/2025")  # Feriado
        nan
    """
    if any_is_empty(date):
        return float("nan")

    try:
        # Converte a data para o formato esperado do arquivo: YYYYMMDD.txt
        data_ref = converter_datas(date)
        nome_arquivo = data_ref.strftime("%Y%m%d.txt")

        # Usa context manager para gerenciamento seguro de recursos (auto-close/quit)
        with ftplib.FTP("ftp.cetip.com.br", timeout=10) as ftp:
            ftp.login()
            ftp.cwd("/MediaCDI")

            linhas = []
            try:
                ftp.retrlines(f"RETR {nome_arquivo}", linhas.append)
            except ftplib.error_perm as e:
                # Código 550 geralmente significa "Arquivo não encontrado"
                if str(e).startswith("550"):
                    logger.warning(f"Arquivo DI não encontrado para {date}: {e}")
                    return float("nan")

                # Se for outro tipo de erro, relança para ser capturado abaixo
                raise

            if not linhas:
                logger.error(f"Arquivo {nome_arquivo} está vazio.")
                return float("nan")

            # Faz o parsing da taxa
            # Formato usual: "00001315" -> 13.15% -> 0.1315
            taxa_bruta = linhas[0].strip()
            taxa = int(taxa_bruta) / 10**CASAS_DECIMAIS_DI_OVER
            return round(taxa, CASAS_DECIMAIS_DI_OVER)

    except ValueError as e:
        logger.error(f"Erro no formato da data para entrada '{date}': {e}")
        raise

    except ftplib.all_errors as e:
        logger.error(f"Erro de conexão ou transferência FTP: {e}")
        raise ConnectionError(f"Falha ao buscar taxa DI via FTP: {e}") from e
