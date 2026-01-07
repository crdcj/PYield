"""
Example of JSON data from B3 API for DI1 contract:
[
    {'SctyQtn': {
        'bottomLmtPric': 12.43,
        'prvsDayAdjstmntPric': 13.396,
        'topLmtPric': 14.675,
        'opngPric': 13.37,
        'minPric': 13.37,
        'maxPric': 13.37,
        'avrgPric': 13.37,
        'curPrc': 13.37},
        'asset': {
            'AsstSummry': {
                'grssAmt': 1657811.68,
                'mtrtyCode': '2030-04-01',
                'opnCtrcts': 36457,
                'tradQty': 7,
                'traddCtrctsQty': 29},
                'code': 'DI1'
            },
            'buyOffer': {'price': 13.38},
            'mkt': {'cd': 'FUT'},
            'sellOffer': {'price': 13.395},
            'symb': 'DI1J30',
            'desc': 'DI DE 1 DIA'},
    {'SctyQtn': {...
"""

import datetime as dt
import logging

import pandas as pd
import polars as pl
import polars.selectors as cs
import requests

from pyield import bday, clock
from pyield.fwd import forwards
from pyield.retry import default_retry

BASE_URL = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation"


# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
INTRADAY_START_TIME = dt.time(9, 16)

# Set up logging
logger = logging.getLogger(__name__)


@default_retry
def _fetch_json(contract_code: str) -> list[dict]:
    url = f"{BASE_URL}/{contract_code}"

    r = requests.get(url, timeout=10)
    r.raise_for_status()  # Check for HTTP request errors
    r.encoding = "utf-8"  # Explicitly set response encoding to utf-8 for consistency

    # Check if the response contains the expected data
    if "Quotation not available" in r.text or "curPrc" not in r.text:
        return []

    return r.json()["Scty"]


def _convert_json(json_data: list[dict]) -> pl.DataFrame:
    # Normalize JSON response into a flat table
    # Polars json_normalize is unstable, so we use Pandas first
    df = pd.json_normalize(json_data).convert_dtypes(dtype_backend="pyarrow")
    return pl.from_pandas(df, nan_to_null=True)


def _process_columns(df: pl.DataFrame) -> pl.DataFrame:
    df.columns = [
        c.replace("SctyQtn.", "").replace("asset.AsstSummry.", "") for c in df.columns
    ]

    rename_map = {
        "symb": "TickerSymbol",
        # "desc": "Description",
        # "asset.code": "AssetCode",
        # "mkt.cd": "MarketCode",
        "bottomLmtPric": "MinLimitRate",
        "prvsDayAdjstmntPric": "PrevSettlementRate",
        "topLmtPric": "MaxLimitRate",
        "opngPric": "OpenRate",
        "minPric": "MinRate",
        "maxPric": "MaxRate",
        "avrgPric": "AvgRate",
        "curPrc": "LastRate",
        "grssAmt": "FinancialVolume",
        "mtrtyCode": "ExpirationDate",
        "opnCtrcts": "OpenContracts",
        "tradQty": "TradeCount",
        "traddCtrctsQty": "TradeVolume",
        "buyOffer.price": "LastAskRate",
        "sellOffer.price": "LastBidRate",
    }
    df = df.select(rename_map.keys()).rename(rename_map, strict=False)
    return df


def _pre_process_df(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.with_columns(
            pl.col("ExpirationDate").str.strptime(
                pl.Date, format="%Y-%m-%d", strict=False
            )
        )
        .drop_nulls(subset=["ExpirationDate"])
        .filter(pl.col("OpenContracts") > 0)  # Remove contracts with zero open interest
        .sort("ExpirationDate")
    )
    return df


def _process_df(df: pl.DataFrame, contract_code: str) -> pl.DataFrame:
    trade_date = bday.last_business_day()
    df = df.with_columns(
        # Remove percentage in all rate columns
        (cs.contains("Rate") / 100).round(5),
        TradeDate=trade_date,
        LastUpdate=(clock.now() - dt.timedelta(minutes=15)),
        DaysToExp=((pl.col("ExpirationDate") - trade_date).dt.total_days()),
    ).filter(pl.col("DaysToExp") > 0)  # Remove expiring contracts

    bdays_to_exp = bday.count(trade_date, df["ExpirationDate"])
    df = df.with_columns(pl.Series(bdays_to_exp).alias("BDaysToExp"))

    if contract_code in {"DI1", "DAP"}:  # Add LastPrice for DI1 and DAP
        fwd_rate = forwards(bdays=df["BDaysToExp"], rates=df["LastRate"])
        byears = pl.col("BDaysToExp") / 252
        last_price = 100_000 / ((1 + pl.col("LastRate")) ** byears)
        df = df.with_columns(
            LastPrice=last_price.round(2),
            ForwardRate=fwd_rate,
        )

    if contract_code == "DI1":  # Add DV01 for DI1
        duration = pl.col("BDaysToExp") / 252
        modified_duration = duration / (1 + pl.col("LastRate"))
        df = df.with_columns(DV01=(0.0001 * modified_duration * pl.col("LastPrice")))
    return df


def _select_and_reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
    all_columns = [
        "TradeDate",
        "LastUpdate",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        "OpenContracts",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "DV01",
        "LastPrice",
        "PrevSettlementRate",
        "MinLimitRate",
        "MaxLimitRate",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "LastAskRate",
        "LastBidRate",
        "LastRate",
        "ForwardRate",
    ]
    reordered_columns = [col for col in all_columns if col in df.columns]
    return df.select(reordered_columns)


def _empty_logger(contract_code: str) -> None:
    date_str = clock.now().strftime("%d-%m-%Y %H:%M")
    logger.warning(
        f"No intraday data available for {contract_code} on {date_str}. "
        f"Returning an empty DataFrame."
    )


def fetch_intraday_df(contract_code: str) -> pl.DataFrame:
    """
    Fetch the latest futures data from B3.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the latest DI futures data.
    """
    try:
        json_data = _fetch_json(contract_code)
        if not json_data:
            _empty_logger(contract_code)
            return pl.DataFrame()

        df = _convert_json(json_data)
        if df.is_empty():
            _empty_logger(contract_code)
            return pl.DataFrame()

        df = _process_columns(df)
        df = _pre_process_df(df)
        df = _process_df(df, contract_code)
        df = _select_and_reorder_columns(df)
        return df
    except Exception as e:
        # 1. Pega Exception genérico (qualquer erro).
        # 2. logger.exception grava o erro E a pilha de chamadas (traceback).
        # 3. Retorna DataFrame vazio para não quebrar a API.
        logger.exception(
            f"CRITICAL: Failed to process {contract_code} for today. Error: {e}"
        )
        return pl.DataFrame()
