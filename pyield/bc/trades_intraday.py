"""
Fetches real-time secondary trading data for domestic Federal Public Debt (FPD)
https://www.bcb.gov.br/htms/selic/selicprecos.asp?frame=1
"""

import datetime as dt
import io
import logging

import polars as pl
import polars.selectors as cs
import requests

from pyield import bday, clock

REALTIME_START_TIME = dt.time(9, 0, 0)
REALTIME_END_TIME = dt.time(22, 0, 0)

logger = logging.getLogger(__name__)

COLUMN_MAP = {
    "//1": ("RowType", None),
    "código título": ("SelicCode", pl.Int64),
    "data vencimento": ("MaturityDate", None),
    "sigla": ("BondType", None),
    "mercado à vista pu último": ("LastPrice", pl.Float64),
    "tx último": ("LastRate", pl.Float64),
    "pu mínimo": ("MinPrice", pl.Float64),
    "tx mínimo": ("MinRate", pl.Float64),
    "pu médio": ("AvgPrice", pl.Float64),
    "tx médio": ("AvgRate", pl.Float64),
    "pu máximo": ("MaxPrice", pl.Float64),
    "tx máximo": ("MaxRate", pl.Float64),
    "totais liquidados operações": ("Trades", pl.Int64),
    "corretagem liquidados operações": ("BrokeredTrades", pl.Int64),
    "títulos": ("Quantity", pl.Int64),
    "corretagem títulos": ("BrokeredQuantity", pl.Int64),
    "financeiro": ("Value", pl.Float64),
    "mercado a termo pu último": ("FwdLastPrice", pl.Float64),
    "tx último_duplicated_0": ("FwdLastRate", pl.Float64),
    "pu mínimo_duplicated_0": ("FwdMinPrice", pl.Float64),
    "tx mínimo_duplicated_0": ("FwdMinRate", pl.Float64),
    "pu médio_duplicated_0": ("FwdAvgPrice", pl.Float64),
    "tx médio_duplicated_0": ("FwdAvgRate", pl.Float64),
    "pu máximo_duplicated_0": ("FwdMaxPrice", pl.Float64),
    "tx máximo_duplicated_0": ("FwdMaxRate", pl.Float64),
    "totais contratados operações": ("FwdTrades", pl.Int64),
    "corretagem contratados operações": ("FwdBrokeredTrades", pl.Int64),
    "títulos_duplicated_0": ("FwdQuantity", pl.Int64),
    "corretagem títulos_duplicated_0": ("FwdBrokeredQuantity", pl.Int64),
    "financeiro_duplicated_0": ("FwdValue", pl.Float64),
}

API_COL_MAPPING = {col: alias for col, (alias, _) in COLUMN_MAP.items()}
DATA_SCHEMA = {
    alias: dtype for _, (alias, dtype) in COLUMN_MAP.items() if dtype is not None
}

FINAL_COLUMN_ORDER = [
    "CollectedAt",
    "SettlementDate",
    "BondType",
    "SelicCode",
    "MaturityDate",
    "MinPrice",
    "AvgPrice",
    "MaxPrice",
    "LastPrice",
    "MinRate",
    "AvgRate",
    "MaxRate",
    "LastRate",
    "Trades",
    "Quantity",
    "Value",
    "BrokeredTrades",
    "BrokeredQuantity",
    "FwdMinPrice",
    "FwdAvgPrice",
    "FwdLastPrice",
    "FwdMaxPrice",
    "FwdLastRate",
    "FwdMinRate",
    "FwdAvgRate",
    "FwdMaxRate",
    "FwdTrades",
    "FwdQuantity",
    "FwdValue",
    "FwdBrokeredTrades",
    "FwdBrokeredQuantity",
]


def _fetch_csv_from_url() -> str:
    """
    Example URL for the CSV file containing intraday trades data:
        https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/02-06-2025
    """
    today = clock.today()
    formatted_date = today.strftime("%d-%m-%Y")
    url = f"https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/{formatted_date}"
    r = requests.get(url, timeout=30)  # API usually takes 10s to respond
    r.raise_for_status()
    r.encoding = "utf-8-sig"  # Handle BOM for UTF-8
    return r.text


def _clean_csv(text: str) -> str:
    rows = text.splitlines()
    # Strip spaces from column names so they match COLUMN_MAP keys
    header = ";".join(col.strip() for col in rows[0].split(";"))
    valid_rows = [header] + [row for row in rows if row.startswith("1;")]
    text = "\n".join(valid_rows)
    text = text.replace(".", "")  # Remove thousands separator
    text = text.replace(",", ".")  # Replace decimal comma with dot
    return text


def _convert_csv_to_df(text: str) -> pl.DataFrame:
    return pl.read_csv(
        io.StringIO(text),
        separator=";",
        null_values="-",
    )


def _process_df(df: pl.DataFrame) -> pl.DataFrame:
    now = clock.now()
    today = now.date()

    df = (
        df.rename(API_COL_MAPPING)
        .cast(DATA_SCHEMA, strict=False)  # type: ignore[call-arg]
        .drop("RowType", strict=False)
        .with_columns(
            pl.col("BondType").str.strip_chars(),
            pl.col("MaturityDate").str.to_date("%d/%m/%Y"),
            cs.contains("Rate").truediv(100).round(6),
            SettlementDate=today,
            CollectedAt=now,
        )
    )

    # 3. Final selection and reordering
    final_columns = [col for col in FINAL_COLUMN_ORDER if col in df.columns]

    return df.select(final_columns)


def is_selic_open() -> bool:
    """Verifica se o mercado está aberto no momento."""
    now = clock.now()
    today = now.date()
    time = now.time()
    is_last_bday = bday.is_business_day(today)
    is_trading_time = REALTIME_START_TIME <= time <= REALTIME_END_TIME

    return is_last_bday and is_trading_time


def tpf_intraday_trades() -> pl.DataFrame:
    """Fetches real-time secondary trading data for domestic Federal Public Debt
    (TPF - títulos públicos federais) from the Central Bank of Brazil (BCB).

    Data is available only during SELIC trading hours (09:00–22:00 BRT) on
    business days. Returns an empty DataFrame outside this window.

    Returns:
        pl.DataFrame: DataFrame with intraday trades. Empty if market is
            closed or on error.

    Output Columns:
        * CollectedAt (datetime): Timestamp of data collection (BRT).
        * SettlementDate (date): Spot market settlement date.
        * BondType (str): Bond ticker (e.g., LFT, LTN, NTN-B).
        * SelicCode (int): SELIC code identifying the bond issue.
        * MaturityDate (date): Bond maturity date.
        * MinPrice (float): Minimum traded price.
        * AvgPrice (float): Average traded price.
        * MaxPrice (float): Maximum traded price.
        * LastPrice (float): Last traded price.
        * MinRate (float): Minimum traded rate (decimal).
        * AvgRate (float): Average traded rate (decimal).
        * MaxRate (float): Maximum traded rate (decimal).
        * LastRate (float): Last traded rate (decimal).
        * Trades (int): Total number of settled trades.
        * Quantity (int): Total bonds traded.
        * Value (float): Total financial value traded (BRL).
        * BrokeredTrades (int): Number of brokered settled trades.
        * BrokeredQuantity (int): Bonds traded via brokers.
        * FwdMinPrice (float): Forward minimum traded price.
        * FwdAvgPrice (float): Forward average traded price.
        * FwdMaxPrice (float): Forward maximum traded price.
        * FwdLastPrice (float): Forward last traded price.
        * FwdMinRate (float): Forward minimum traded rate (decimal).
        * FwdAvgRate (float): Forward average traded rate (decimal).
        * FwdMaxRate (float): Forward maximum traded rate (decimal).
        * FwdLastRate (float): Forward last traded rate (decimal).
        * FwdTrades (int): Forward total trades contracted.
        * FwdQuantity (int): Forward total bonds traded.
        * FwdValue (float): Forward total value traded (BRL).
        * FwdBrokeredTrades (int): Forward brokered trades contracted.
        * FwdBrokeredQuantity (int): Forward bonds traded via brokers.
    """
    if not is_selic_open():
        logger.info("Market is closed. Returning empty DataFrame.")
        return pl.DataFrame()

    try:
        raw_text = _fetch_csv_from_url()
        cleaned_text = _clean_csv(raw_text)
        if not cleaned_text:
            logger.warning("No data found in the FPD intraday trades.")
            return pl.DataFrame()

        df = _convert_csv_to_df(cleaned_text)
        df = _process_df(df)

        value = df["Value"].sum() / 10**9
        logger.info(f"Fetched {value:,.1f} billion BRL in FPD intraday trades.")
        return df
    except Exception as e:
        logger.exception(
            f"Error fetching data from BCB: {e}. Returning empty DataFrame."
        )
        return pl.DataFrame()
