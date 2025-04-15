import datetime as dt
import logging
from typing import Literal
from zoneinfo import ZoneInfo

import pandas as pd

from pyield import bday
from pyield import date_converter as dc
from pyield.b3.futures.historical import fetch_bmf_data
from pyield.b3.futures.intraday import fetch_intraday_df
from pyield.b3.futures.xml import fetch_xml_data
from pyield.date_converter import DateScalar

BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")
ContractOptions = Literal["DI1", "DDI", "FRC", "DAP", "DOL", "WDO", "IND", "WIN"]
logger = logging.getLogger(__name__)


# Apesar do pregão terminar às 18:00h, os dados consolidados são disponibilizados
# normalmente somente após as 20:00h.
HISTORICAL_START_TIME = dt.time(20, 0)

# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
INTRADAY_START_TIME = dt.time(9, 16)


def _is_intraday_date(date: pd.Timestamp) -> bool:
    """Verifica se os dados intraday estão disponíveis."""

    last_bday = bday.last_business_day().date()
    if date.date() != last_bday:
        return False

    # Pregão não abre em feriados ou datas especiais
    today = dt.datetime.now(BZ_TIMEZONE).date()
    special_closed_dates = {  # Datas especiais
        dt.date(today.year, 12, 24),  # Véspera de Natal
        dt.date(today.year, 12, 31),  # Véspera de Ano Novo
    }
    if today in special_closed_dates:
        return False

    return True


def _get_historical_data(
    contract_code: str,
    date: pd.Timestamp,
) -> pd.DataFrame:
    """Fetches historical data for a specified futures contract and reference date."""
    # First, try to fetch the data from BMF legacy service
    df = fetch_bmf_data(contract_code, date)
    if not df.empty:
        # If data is available from BMF, return it
        return df

    # If BMF data is not available, try to fetch the full XML report
    df = fetch_xml_data(date, contract_code, "PR")

    return df


def futures(
    contract_code: ContractOptions | str,
    date: DateScalar,
) -> pd.DataFrame:
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
        pd.DataFrame: DataFrame containing the fetched data for the specified futures
            contract.

    Raises:
        ValueError: If the futures contract code is not recognized or supported.

    Examples:
        >>> futures("DI1", "31-05-2024")
            TradeDate TickerSymbol  ... SettlementRate  ForwardRate
        0  2024-05-31       DI1M24  ...        0.10399      0.10399
        1  2024-05-31       DI1N24  ...         0.1039     0.103896
        2  2024-05-31       DI1Q24  ...         0.1037     0.103517
        3  2024-05-31       DI1U24  ...         0.1036       0.1034
        ...

        >>> futures("DAP", "31-05-2024")
            TradeDate TickerSymbol  ... SettlementRate  ForwardRate
        0  2024-05-31       DAPM24  ...         0.0555       0.0555
        1  2024-05-31       DAPN24  ...        0.07524     0.086254
        2  2024-05-31       DAPQ24  ...         0.0885     0.106631
        3  2024-05-31       DAPU24  ...         0.0855     0.078171
        ...
    """
    selected_contract = str(contract_code).upper()
    converted_date = dc.convert_input_dates(date)

    if _is_intraday_date(converted_date):
        # É um dia de negociação intraday
        # Verificar se o mercado está aberto
        time = dt.datetime.now(BZ_TIMEZONE).time()
        if time < INTRADAY_START_TIME:  # Mercado não está aberto ainda
            logger.warning("Market is not open yet. Returning empty DataFrame. ")
            return pd.DataFrame()

        # Mercado está aberto, verificar se os dados intraday estão disponíveis
        df = fetch_intraday_df(selected_contract)

        # Existe a chance de que os dados históricos estejam disponíveis após as 20:00h
        if time >= HISTORICAL_START_TIME:
            # If historical data is available, we can use it instead of intraday data
            df_hist = _get_historical_data(selected_contract, converted_date)
            if not df_hist.empty:
                logger.info("Consolidated data is already available and will be used.")
                df = df_hist
        return df
    else:
        # Historical path
        return _get_historical_data(selected_contract, converted_date)


__all__ = [
    "futures",
    "ContractOptions",
]
