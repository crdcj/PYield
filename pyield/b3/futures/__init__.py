import logging

import polars as pl

import pyield._internal.converters as cv
from pyield import clock
from pyield._internal.types import DateLike, any_is_empty
from pyield.b3._contracts import normalizar_codigos_contrato
from pyield.b3._validar_pregao import data_negociacao_valida
from pyield.b3.futures import historical, intraday

logger = logging.getLogger(__name__)


def _buscar_intraday_ou_historico(
    data_negociacao,
    codigos_contrato: list[str],
    full_report: bool,
) -> pl.DataFrame:
    horario_atual = clock.now().time()
    if horario_atual < intraday.HORA_INICIO_INTRADAY:
        return pl.DataFrame()

    if horario_atual >= intraday.HORA_FIM_INTRADAY:
        df_historico = historical.historical(
            data_negociacao, codigos_contrato, full_report
        )
        if not df_historico.is_empty():
            logger.info("Dados consolidados disponíveis. Usando histórico.")
            return df_historico

    dataframes_intraday = [intraday.intraday(codigo) for codigo in codigos_contrato]
    dataframes_intraday = [df for df in dataframes_intraday if not df.is_empty()]
    if not dataframes_intraday:
        return pl.DataFrame()
    if len(dataframes_intraday) == 1:
        return dataframes_intraday[0]
    return pl.concat(dataframes_intraday, how="diagonal_relaxed").sort("TickerSymbol")


def futures(
    date: DateLike,
    contract_code: str | list[str],
    full_report: bool = False,
) -> pl.DataFrame:
    """Busca dados de um contrato futuro da B3 para a data de referência.

    Args:
        date: Data de referência para consulta.
        contract_code: Código do contrato futuro na B3 ou coleção de códigos.
            Exemplos de códigos válidos:
            - Juros: DI1, DDI, OC1, DAP, IAP
            - Moedas: DOL, WDO, EUR, GBR, JAP, CNY
            - Índices: IND, WIN, ISP, WSP
            - Commodities: BGI, CCM, ICF, CNL, SJC, SOY, ETH, GLD
        full_report: Se False (padrão), usa o simplified price report (SPR),
            arquivo leve (~2 KB) com apenas preços de ajuste. Se True, usa o
            price report completo (PR, ~2 MB) com todos os dados de negociação.
            Relevante apenas quando o dado não está no cache.

    Returns:
        DataFrame Polars com os dados do contrato informado.

    Notes:
        Os contratos DI1, DDI, FRC, FRO, DAP, DOL, WDO, IND e WIN possuem
        histórico pré-cacheado (desde 2018) e são retornados instantaneamente.
        Para os demais contratos, os dados são baixados diretamente da B3 a
        cada chamada, o que pode ser mais lento.

    Examples:
        >>> df = futures("31-05-2024", "DI1")
        >>> df = futures("31-05-2024", "DAP")

        Véspera de Natal e Ano Novo não têm pregão:

        >>> futures("24-12-2024", "DI1").is_empty()
        True
        >>> futures("31-12-2024", "DI1").is_empty()
        True

        Data futura e fim de semana retornam DataFrame vazio:

        >>> import datetime as dt
        >>> amanha = dt.date.today() + dt.timedelta(days=1)
        >>> futures(amanha, "DI1").is_empty()
        True
        >>> futures("04-01-2025", "DI1").is_empty()  # sábado
        True

    """
    if any_is_empty(date, contract_code):
        return pl.DataFrame()

    codigos_contrato = normalizar_codigos_contrato(contract_code)
    if not codigos_contrato:
        return pl.DataFrame()

    data_negociacao = cv.converter_datas(date)
    if not data_negociacao_valida(data_negociacao):
        return pl.DataFrame()

    if intraday.data_intraday_valida(data_negociacao):
        return _buscar_intraday_ou_historico(
            data_negociacao=data_negociacao,
            codigos_contrato=codigos_contrato,
            full_report=full_report,
        )

    return historical.historical(data_negociacao, codigos_contrato, full_report)


__all__ = ["futures"]
