import logging
from typing import Literal

import polars as pl

import pyield._internal.converters as cv
from pyield import clock
from pyield._internal.types import DateLike, any_is_empty
from pyield.b3.futures import historical as historico
from pyield.b3.futures import intraday as intradiario
from pyield.b3.validar_pregao import data_negociacao_valida

TipoFonte = Literal["PR", "SPR"]

OpcoesContrato = Literal[
    "DI1",
    "DDI",
    "FRC",
    "FRO",
    "DAP",
    "DOL",
    "WDO",
    "IND",
    "WIN",
    "CPM",
]

logger = logging.getLogger(__name__)


def futures(
    date: DateLike,
    contract_code: OpcoesContrato | str,
    source_type: TipoFonte = "SPR",
) -> pl.DataFrame:
    """Busca dados de um contrato futuro da B3 para a data de referência.

    Args:
        date: Data de referência para consulta.
        contract_code: Código do contrato futuro na B3.
        source_type: Tipo do arquivo de preços da B3. 'SPR' (padrão) para
            settlement price report (arquivo leve, ~2 KB) e 'PR' para price
            report completo (~2 MB). Relevante apenas quando o dado não está
            no cache.

    Returns:
        DataFrame Polars com os dados do contrato informado.

    Examples:
        >>> df = futures("31-05-2024", "DI1")
        >>> {"TradeDate", "TickerSymbol", "ExpirationDate", "SettlementRate"}.issubset(
        ...     set(df.columns)
        ... )
        True
        >>> df.shape[0] > 0
        True

        >>> df = futures("31-05-2024", "DAP")
        >>> {"TradeDate", "TickerSymbol", "ExpirationDate", "SettlementRate"}.issubset(
        ...     set(df.columns)
        ... )
        True
        >>> df.shape[0] > 0
        True

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

    data_negociacao = cv.converter_datas(date)
    if not data_negociacao_valida(data_negociacao):
        logger.warning(
            "A data %s não é válida. Retornando DataFrame vazio.",
            data_negociacao,
        )
        return pl.DataFrame()

    contrato_selecionado = str(contract_code).upper()

    if intradiario.data_intraday_valida(data_negociacao):
        horario_atual = clock.now().time()
        if horario_atual < intradiario.HORA_INICIO_INTRADAY:
            logger.warning("Mercado ainda não abriu. Retornando DataFrame vazio.")
            return pl.DataFrame()

        if horario_atual >= intradiario.HORA_FIM_INTRADAY:
            df_historico = historico.historical(
                data_negociacao, contrato_selecionado, source_type
            )
            if not df_historico.is_empty():
                logger.info("Dados consolidados disponíveis. Usando histórico.")
                return df_historico

        return intradiario.intraday(contrato_selecionado)

    return historico.historical(data_negociacao, contrato_selecionado, source_type)


__all__ = ["futures"]
