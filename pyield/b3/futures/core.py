import datetime as dt
import logging
from typing import Literal

import polars as pl

import pyield._internal.converters as cv
import pyield.b3.common as cm
import pyield.b3.futures.historical.core as hcore
from pyield import clock
from pyield._internal.types import DateLike, any_is_empty
from pyield.b3.futures.intraday import fetch_intraday_df

OpcoesContrato = Literal[
    "DI1",
    "DDI",
    "FRC",
    "DAP",
    "DOL",
    "WDO",
    "IND",
    "WIN",
    "CPM",
]
logger = logging.getLogger(__name__)


# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
HORA_INICIO_INTRADAY = dt.time(9, 16)
# Pregão fecha às 18:00h, momento em que os dados consolidados começam a ser preparados.
HORA_FIM_INTRADAY = dt.time(18, 30)


def _data_intraday_valida(data_verificacao: dt.date) -> bool:
    """Verifica se a data é um dia de negociação intraday."""
    # Primeiro valida regra geral de dia futuro / não útil / datas especiais
    if not cm.data_negociacao_valida(data_verificacao):
        return False

    # Intraday só existe para 'hoje'
    hoje_brasil = clock.today()
    if data_verificacao != hoje_brasil:
        return False

    return True


def futures(
    date: DateLike,
    contract_code: OpcoesContrato | str,
) -> pl.DataFrame:
    """
    Fetches data for a specified futures contract based on type and reference date.

    Args:
        contract_code (str): The B3 futures contract code identifying the derivative.
            Supported contract codes are:
            - "DI1": One-day Interbank Deposit Futures (Futuro de DI) from B3.
            - "DDI": DI x U.S. Dollar Spread Futures (Futuro de Cupom Cambial) from B3.
            - "FRC": Forward Rate Agreement (FRA).
            - "FRO": FRA DE CUPOM CAMBIAL EM OC1
            - "DAP": DI x IPCA Spread Futures.
            - "DOL": U.S. Dollar Futures from B3.
            - "WDO": Mini U.S. Dollar Futures from B3.
            - "IND": Ibovespa Futures from B3.
            - "WIN": Mini Ibovespa Futures from B3.
        date (DateLike): The reference date for fetching the data.

    Returns:
        pl.DataFrame: DataFrame containing the fetched data for the specified futures
            contract.

    Raises:
        ValueError: If the futures contract code is not recognized or supported.

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

    """  # noqa: E501
    if any_is_empty(date, contract_code):
        return pl.DataFrame()
    data_negociacao = cv.converter_datas(date)

    # Validação centralizada (evita chamadas desnecessárias às APIs B3)
    if not cm.data_negociacao_valida(data_negociacao):
        logger.warning(
            "A data %s não é válida. Retornando DataFrame vazio.",
            data_negociacao,
        )
        return pl.DataFrame()

    contrato_selecionado = str(contract_code).upper()

    if _data_intraday_valida(data_negociacao):
        # É um dia de negociação intraday
        horario_atual = clock.now().time()
        if horario_atual < HORA_INICIO_INTRADAY:  # Mercado não está aberto ainda
            logger.warning("Mercado ainda não abriu. Retornando DataFrame vazio.")
            return pl.DataFrame()

        # Existe a chance de que os dados consolidados estejam disponíveis após as 18h
        if horario_atual >= HORA_FIM_INTRADAY:
            df_hist = hcore.buscar_df_historico(data_negociacao, contrato_selecionado)
            if not df_hist.is_empty():
                logger.info("Dados consolidados disponíveis. Usando histórico.")
                return df_hist

        # Mercado está aberto e não há dados consolidados disponíveis ainda
        return fetch_intraday_df(contrato_selecionado)

    else:  # É um dia histórico
        return hcore.buscar_df_historico(data_negociacao, contrato_selecionado)
