import datetime as dt
import logging
from typing import Literal

import polars as pl

import pyield.converters as cv
from pyield import clock
from pyield.b3.common import is_trade_date_valid
from pyield.b3.futures.historical.core import fetch_historical_df
from pyield.b3.futures.intraday import fetch_intraday_df
from pyield.types import DateLike, has_nullable_args

ContractOptions = Literal["DI1", "DDI", "FRC", "DAP", "DOL", "WDO", "IND", "WIN"]
logger = logging.getLogger(__name__)


# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
INTRADAY_START_TIME = dt.time(9, 16)
# Pregão fecha às 18:00h, momento em que os dados consolidados começam a ser preparados.
INTRADAY_END_TIME = dt.time(18, 30)


def _is_intraday_date(check_date: dt.date) -> bool:
    """Check if a date is a trading day."""
    # Primeiro valida regra geral de dia futuro / não útil / datas especiais
    if not is_trade_date_valid(check_date):
        return False

    # Intraday só existe para 'hoje'
    today_bz = clock.today()
    if check_date != today_bz:
        return False

    return True


def futures(
    date: DateLike,
    contract_code: ContractOptions | str,
) -> pl.DataFrame:
    """
    Fetches data for a specified futures contract based on type and reference date.

    Args:
        contract_code (str): The B3 futures contract code identifying the derivative.
            Supported contract codes are:
            - "DI1": One-day Interbank Deposit Futures (Futuro de DI) from B3.
            - "DDI": DI x U.S. Dollar Spread Futures (Futuro de Cupom Cambial) from B3.
            - "FRC": Forward Rate Agreement (FRA) from B3.
            - "DAP": DI x IPCA Spread Futures.
            - "DOL": U.S. Dollar Futures from B3.
            - "WDO": Mini U.S. Dollar Futures from B3.
            - "IND": Ibovespa Futures from B3.
            - "WIN": Mini Ibovespa Futures from B3.
        date (DateLike): The date for which to fetch the data.
            If the reference date is a string, it should be in 'DD-MM-YYYY' format.

    Returns:
        pl.DataFrame: DataFrame containing the fetched data for the specified futures
            contract.

    Raises:
        ValueError: If the futures contract code is not recognized or supported.

    Examples:
        >>> futures("31-05-2024", "DI1")
        shape: (40, 20)
        ┌────────────┬──────────────┬────────────────┬────────────┬───┬──────────────┬───────────┬────────────────┬─────────────┐
        │ TradeDate  ┆ TickerSymbol ┆ ExpirationDate ┆ BDaysToExp ┆ … ┆ CloseBidRate ┆ CloseRate ┆ SettlementRate ┆ ForwardRate │
        │ ---        ┆ ---          ┆ ---            ┆ ---        ┆   ┆ ---          ┆ ---       ┆ ---            ┆ ---         │
        │ date       ┆ str          ┆ date           ┆ i64        ┆   ┆ f64          ┆ f64       ┆ f64            ┆ f64         │
        ╞════════════╪══════════════╪════════════════╪════════════╪═══╪══════════════╪═══════════╪════════════════╪═════════════╡
        │ 2024-05-31 ┆ DI1M24       ┆ 2024-06-03     ┆ 1          ┆ … ┆ 0.10404      ┆ 0.10404   ┆ 0.10399        ┆ 0.10399     │
        │ 2024-05-31 ┆ DI1N24       ┆ 2024-07-01     ┆ 21         ┆ … ┆ 0.1039       ┆ 0.10386   ┆ 0.1039         ┆ 0.103896    │
        │ 2024-05-31 ┆ DI1Q24       ┆ 2024-08-01     ┆ 44         ┆ … ┆ 0.10374      ┆ 0.10374   ┆ 0.1037         ┆ 0.103517    │
        │ 2024-05-31 ┆ DI1U24       ┆ 2024-09-02     ┆ 66         ┆ … ┆ 0.10365      ┆ 0.10355   ┆ 0.1036         ┆ 0.1034      │
        │ 2024-05-31 ┆ DI1V24       ┆ 2024-10-01     ┆ 87         ┆ … ┆ 0.1036       ┆ 0.10355   ┆ 0.1036         ┆ 0.1036      │
        │ …          ┆ …            ┆ …              ┆ …          ┆ … ┆ …            ┆ …         ┆ …              ┆ …           │
        │ 2024-05-31 ┆ DI1F35       ┆ 2035-01-02     ┆ 2654       ┆ … ┆ 0.1193       ┆ 0.1192    ┆ 0.11907        ┆ 0.12179     │
        │ 2024-05-31 ┆ DI1F36       ┆ 2036-01-02     ┆ 2903       ┆ … ┆ null         ┆ null      ┆ 0.11887        ┆ 0.11674     │
        │ 2024-05-31 ┆ DI1F37       ┆ 2037-01-02     ┆ 3156       ┆ … ┆ null         ┆ null      ┆ 0.11887        ┆ 0.11887     │
        │ 2024-05-31 ┆ DI1F38       ┆ 2038-01-04     ┆ 3405       ┆ … ┆ null         ┆ null      ┆ 0.11887        ┆ 0.11887     │
        │ 2024-05-31 ┆ DI1F39       ┆ 2039-01-03     ┆ 3656       ┆ … ┆ null         ┆ null      ┆ 0.11887        ┆ 0.11887     │
        └────────────┴──────────────┴────────────────┴────────────┴───┴──────────────┴───────────┴────────────────┴─────────────┘

        >>> futures("31-05-2024", "DAP")
        shape: (22, 19)
        ┌────────────┬──────────────┬────────────────┬────────────┬───┬──────────────┬───────────┬────────────────┬─────────────┐
        │ TradeDate  ┆ TickerSymbol ┆ ExpirationDate ┆ BDaysToExp ┆ … ┆ CloseBidRate ┆ CloseRate ┆ SettlementRate ┆ ForwardRate │
        │ ---        ┆ ---          ┆ ---            ┆ ---        ┆   ┆ ---          ┆ ---       ┆ ---            ┆ ---         │
        │ date       ┆ str          ┆ date           ┆ i64        ┆   ┆ f64          ┆ f64       ┆ f64            ┆ f64         │
        ╞════════════╪══════════════╪════════════════╪════════════╪═══╪══════════════╪═══════════╪════════════════╪═════════════╡
        │ 2024-05-31 ┆ DAPM24       ┆ 2024-06-17     ┆ 11         ┆ … ┆ null         ┆ null      ┆ 0.0555         ┆ 0.0555      │
        │ 2024-05-31 ┆ DAPN24       ┆ 2024-07-15     ┆ 31         ┆ … ┆ null         ┆ null      ┆ 0.07524        ┆ 0.086254    │
        │ 2024-05-31 ┆ DAPQ24       ┆ 2024-08-15     ┆ 54         ┆ … ┆ null         ┆ 0.0885    ┆ 0.0885         ┆ 0.106631    │
        │ 2024-05-31 ┆ DAPU24       ┆ 2024-09-16     ┆ 76         ┆ … ┆ null         ┆ 0.0865    ┆ 0.0855         ┆ 0.078171    │
        │ 2024-05-31 ┆ DAPV24       ┆ 2024-10-15     ┆ 97         ┆ … ┆ null         ┆ null      ┆ 0.07932        ┆ 0.057247    │
        │ …          ┆ …            ┆ …              ┆ …          ┆ … ┆ …            ┆ …         ┆ …              ┆ …           │
        │ 2024-05-31 ┆ DAPQ40       ┆ 2040-08-15     ┆ 4064       ┆ … ┆ null         ┆ 0.0609    ┆ 0.06099        ┆ 0.060553    │
        │ 2024-05-31 ┆ DAPK45       ┆ 2045-05-15     ┆ 5251       ┆ … ┆ null         ┆ 0.0619    ┆ 0.0588         ┆ 0.051336    │
        │ 2024-05-31 ┆ DAPQ50       ┆ 2050-08-15     ┆ 6566       ┆ … ┆ null         ┆ 0.0605    ┆ 0.06086        ┆ 0.069126    │
        │ 2024-05-31 ┆ DAPK55       ┆ 2055-05-17     ┆ 7755       ┆ … ┆ null         ┆ 0.0646    ┆ 0.06022        ┆ 0.056693    │
        │ 2024-05-31 ┆ DAPQ60       ┆ 2060-08-16     ┆ 9072       ┆ … ┆ null         ┆ null      ┆ 0.05821        ┆ 0.046451    │
        └────────────┴──────────────┴────────────────┴────────────┴───┴──────────────┴───────────┴────────────────┴─────────────┘

    """  # noqa: E501
    if has_nullable_args(date, contract_code):
        return pl.DataFrame()
    trade_date = cv.convert_dates(date)

    # Validação centralizada (evita chamadas desnecessárias às APIs B3)
    if not is_trade_date_valid(trade_date):
        logger.warning(f"{trade_date} is not a valid date. Returning empty DataFrame.")
        return pl.DataFrame()

    selected_contract = str(contract_code).upper()

    if _is_intraday_date(trade_date):
        # É um dia de negociação intraday
        time = clock.now().time()
        if time < INTRADAY_START_TIME:  # Mercado não está aberto ainda
            logger.warning("Market is not open yet. Returning an empty DataFrame. ")
            return pl.DataFrame()

        # Existe a chance de que os dados consolidados estejam disponíveis após as 18h
        if time >= INTRADAY_END_TIME:
            df_hist = fetch_historical_df(trade_date, selected_contract)
            if not df_hist.is_empty():
                logger.info("Consolidated data is already available and will be used.")
                return df_hist

        # Mercado está aberto e não há dados consolidados disponíveis ainda
        return fetch_intraday_df(selected_contract)

    else:  # É um dia histórico
        return fetch_historical_df(trade_date, selected_contract)
