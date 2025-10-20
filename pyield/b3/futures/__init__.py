import datetime as dt
import logging
from typing import Literal

import polars as pl

import pyield.converters as cv
from pyield import bday
from pyield.b3.futures.historical import fetch_bmf_data
from pyield.b3.futures.intraday import fetch_intraday_df
from pyield.b3.futures.xml import fetch_xml_data
from pyield.config import TIMEZONE_BZ
from pyield.types import DateScalar, has_null_args

ContractOptions = Literal["DI1", "DDI", "FRC", "DAP", "DOL", "WDO", "IND", "WIN"]
logger = logging.getLogger(__name__)


# Apesar do pregão terminar às 18:00h, os dados consolidados são disponibilizados
# normalmente somente após as 20:00h.
HISTORICAL_START_TIME = dt.time(20, 0)

# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
INTRADAY_START_TIME = dt.time(9, 16)


def _validate_reference_date(trade_date: dt.date) -> bool:
    """Valida se a data de referência é utilizável para consulta.

    Critérios:
    - Deve ser um dia útil brasileiro.
    - Não pode estar no futuro (maior que a data corrente no Brasil).

    Retorna True se válida, False caso contrário (e loga um aviso).
    """
    today_bz = dt.datetime.now(TIMEZONE_BZ).date()
    if trade_date > today_bz:
        logger.warning(
            "The provided date %s is in the future. Returning an empty DataFrame.",
            trade_date,
        )
        return False
    if not bday.is_business_day(trade_date):
        logger.warning(
            "The provided date %s is not a business day. Returning an empty DataFrame.",
            trade_date,
        )
        return False

    # Não tem pregão na véspera de Natal e Ano Novo
    special_closed_dates = {  # Datas especiais
        dt.date(trade_date.year, 12, 24),  # Véspera de Natal
        dt.date(trade_date.year, 12, 31),  # Véspera de Ano Novo
    }
    if trade_date in special_closed_dates:
        logger.warning(
            "There is no trading session before Christmas and New Year's Eve: %s. "
            + "Returning an empty DataFrame.",
            trade_date,
        )
        return False

    return True


def _is_trading_day(check_date: dt.date) -> bool:
    """Check if a date is a trading day."""
    # Primeiro valida regra geral de dia futuro / não útil / datas especiais
    if not _validate_reference_date(check_date):
        return False

    # Intraday só existe para 'hoje'
    today_bz = dt.datetime.now(TIMEZONE_BZ).date()
    if check_date != today_bz:
        return False
    return True


def _get_historical_data(
    contract_code: str,
    date: dt.date,
) -> pl.DataFrame:
    """Fetches historical data for a specified futures contract and reference date."""
    # First, try to fetch the data from BMF legacy service
    df = fetch_bmf_data(contract_code, date)
    if not df.is_empty():  # If data is found from BMF
        return df

    # If BMF data is not available, try to fetch the full XML report
    return fetch_xml_data(date, contract_code, "PR")


def futures(
    contract_code: ContractOptions | str,
    date: DateScalar,
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
        date (DateScalar): The date for which to fetch the data.
            If the reference date is a string, it should be in 'DD-MM-YYYY' format.

    Returns:
        pl.DataFrame: DataFrame containing the fetched data for the specified futures
            contract.

    Raises:
        ValueError: If the futures contract code is not recognized or supported.

    Examples:
        >>> futures("DI1", "31-05-2024")
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

        >>> futures("DAP", "31-05-2024")
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
    if has_null_args(contract_code, date):
        return pl.DataFrame()
    trade_date = cv.convert_dates(date)

    # Validação centralizada (evita chamadas desnecessárias às APIs B3)
    if not _validate_reference_date(trade_date):
        return pl.DataFrame()

    selected_contract = str(contract_code).upper()

    if _is_trading_day(trade_date):
        # É um dia de negociação intraday
        time = dt.datetime.now(TIMEZONE_BZ).time()
        if time < INTRADAY_START_TIME:  # Mercado não está aberto ainda
            logger.warning("Market is not open yet. Returning an empty DataFrame. ")
            return pl.DataFrame()

        # Existe a chance de que os dados consolidados estejam disponíveis após as 20h
        if time >= HISTORICAL_START_TIME:
            df_hist = _get_historical_data(selected_contract, trade_date)
            if not df_hist.is_empty():
                logger.info("Consolidated data is already available and will be used.")
                return df_hist

        # Mercado está aberto e não há dados consolidados disponíveis ainda
        return fetch_intraday_df(selected_contract)

    else:  # É um dia histórico
        return _get_historical_data(selected_contract, trade_date)


__all__ = [
    "futures",
    "ContractOptions",
]
