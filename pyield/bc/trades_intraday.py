import datetime as dt
import io
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pyield import bday

BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")
REALTIME_START_TIME = dt.time(9, 0, 0)
REALTIME_END_TIME = dt.time(22, 0, 0)

logger = logging.getLogger(__name__)
test_file = Path(__file__).parent.parent.parent / "tests/data/negocios-registrados.csv"


def _fetch_csv_from_url() -> str:
    FILE_URL = "https://www3.bcb.gov.br/novoselic/NegociosRegistradosDownload"
    r = requests.get(FILE_URL, timeout=30)  # API usually takes 10s to respond
    r.raise_for_status()
    r.encoding = "iso-8859-15"
    return r.text


def _clean_csv(text: str) -> str:
    """Clean the CSV text data by removing unnecessary rows."""
    rows = text.splitlines()
    valid_rows = [row for row in rows if row.startswith("1;")]
    return "\n".join(valid_rows)


def _convert_csv_to_df(text: str) -> pd.DataFrame:
    return pd.read_csv(
        io.StringIO(text),
        sep=";",
        decimal=",",
        thousands=".",
        dtype_backend="numpy_nullable",
        header=None,
        na_values="-",
    )


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    col_names_by_index = {
        0: "RowType",  # column indicating row type (filtered with "1")
        1: "SelicCode",  # código título
        2: "MaturityDate",  # data vencimento
        3: "BondType",  # sigla
        4: "LastPrice",  # pu último (mercado à vista)
        5: "LastRate",  # tx último (mercado à vista)
        6: "MinPrice",  # pu mínimo (mercado à vista)
        7: "MinRate",  # tx mínimo (mercado à vista)
        8: "AvgPrice",  # pu médio (mercado à vista)
        9: "AvgRate",  # tx médio (mercado à vista)
        10: "MaxPrice",  # pu máximo (mercado à vista)
        11: "MaxRate",  # tx máximo (mercado à vista)
        12: "Trades",  # totais liquidados operações (mercado à vista)
        13: "BrokeredTrades",  # corretagem liquidados operações (merc, à vista)
        14: "Quantity",  # títulos (mercado à vista)
        15: "BrokeredQuantity",  # corretagem títulos (mercado à vista)
        16: "Volume",  # financeiro (mercado à vista)
        17: "ForwardLastPrice",  # pu último (mercado a termo)
        18: "ForwardLastRate",  # tx último (mercado a termo)
        19: "ForwardMinPrice",  # pu mínimo (mercado a termo)
        20: "ForwardMinRate",  # tx mínimo (mercado a termo)
        21: "ForwardAvgPrice",  # pu médio (mercado a termo)
        22: "ForwardAvgRate",  # tx médio (mercado a termo)
        23: "ForwardMaxPrice",  # pu máximo (mercado a termo)
        24: "ForwardMaxRate",  # tx máximo (mercado a termo)
        25: "ForwardTrades",  # totais contratados operações (mercado a termo)
        26: "ForwardBrokeredTrades",  # corretagem contratados operações (merc. a termo)
        27: "ForwardQuantity",  # títulos (mercado a termo)
        28: "ForwardBrokeredQuantity",  # corretagem titulos (mercado a termo)
        29: "ForwardVolume",  # financeiro (mercado a termo)
    }
    df = df.rename(columns=col_names_by_index)

    df["BondType"] = df["BondType"].str.strip()
    df["MaturityDate"] = pd.to_datetime(df["MaturityDate"], format="%d-%m-%Y")
    today = dt.datetime.now(BZ_TIMEZONE).date()
    df["SettlementDate"] = pd.Timestamp(today)

    # Remove percentage from rate columns
    rate_cols = [col for col in df.columns if "Rate" in col]
    for col in rate_cols:
        df[col] = (df[col] / 100).round(6)

    return df


def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    reorder_columns = [
        # "RowType",
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
        "Volume",
        "BrokeredTrades",
        "BrokeredQuantity",
        "ForwardMinPrice",
        "ForwardAvgPrice",
        "ForwardLastPrice",
        "ForwardMaxPrice",
        "ForwardLastRate",
        "ForwardMinRate",
        "ForwardAvgRate",
        "ForwardMaxRate",
        "ForwardTrades",
        "ForwardQuantity",
        "ForwardVolume",
        "ForwardBrokeredTrades",
        "ForwardBrokeredQuantity",
    ]

    column_order = [col for col in reorder_columns if col in df.columns]
    return df[column_order].copy()


def is_selic_open() -> bool:
    """Verifica se o mercado está aberto no momento."""
    now = dt.datetime.now(BZ_TIMEZONE)
    today = now.date()
    time = now.time()
    is_last_bday = bday.is_business_day(today)
    is_trading_time = REALTIME_START_TIME <= time <= REALTIME_END_TIME

    return is_last_bday and is_trading_time


def fpd_intraday_trades() -> pd.DataFrame:
    """Fetches real-time secondary trading data for domestic Federal Public Debt (FDP)
    from the Central Bank of Brazil (BCB).
    Returns a DataFrame with the latest trades.
    """
    if not is_selic_open():
        logger.info("Market is closed. Returning empty DataFrame.")
        return pd.DataFrame()

    try:
        raw_text = _fetch_csv_from_url()
        # raw_text = test_file.read_text(encoding="iso-8859-15")
        cleaned_text = _clean_csv(raw_text)
        if not cleaned_text:
            logger.warning("No data found in the FPD intraday trades.")
            return pd.DataFrame()
        df = _convert_csv_to_df(cleaned_text)
        df = _process_df(df)
        df = _reorder_columns(df)
        volume = df["Volume"].sum() / 10**9
        logger.info(f"Fetched {volume:,.1f} billion BRL in FPD intraday trades.")
        return df
    except Exception:
        logger.exception("Error fetching data from BCB. Returning empty DataFrame.")
        return pd.DataFrame()
