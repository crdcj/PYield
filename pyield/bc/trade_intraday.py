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

test_file = Path("/home/crcj/github/PYield/dev/negocios-registrados.csv")


def _fetch_csv_from_url() -> str:
    FILE_URL = "https://www3.bcb.gov.br/novoselic/NegociosRegistradosDownload"
    r = requests.get(FILE_URL)
    r.raise_for_status()
    r.encoding = "iso-8859-15"
    return r.text


def _clean_csv(text: str) -> str:
    """Clean the CSV text data by removing unnecessary rows."""
    rows = text.splitlines()
    clean_rows = [row for row in rows if row.startswith("1;")]
    return "\n".join(clean_rows)


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
    col_names = {
        0: "codigo_negociacao",
        1: "codigo_titulo",
        2: "vencimento",
        3: "titulo",
        4: "pu_ultimo",
        5: "tx_ultima",
        6: "pu_min",
        7: "tx_min",
        8: "pu_medio",
        9: "tx_media",
        10: "pu_max",
        11: "tx_max",
        12: "negocios",
        13: "negocios_corretagem",
        14: "quantidade",
        15: "quantidade_corretagem",
        16: "financeiro",
        17: "pu_ultimo_termo",
        18: "tx_ultima_termo",
        19: "pu_min_termo",
        20: "tx_min_termo",
        21: "pu_medio_termo",
        22: "tx_media_termo",
        23: "pu_max_termo",
        24: "tx_max_termo",
        25: "negocios_termo",
        26: "negocios_corretagem_termo",
        27: "quantidade_termo",
        28: "quantidade_corretagem_termo",
        29: "financeiro_termo",
    }
    df = df.rename(columns=col_names)
    df["titulo"] = df["titulo"].str.strip()
    df["vencimento"] = pd.to_datetime(df["vencimento"], format="%d-%m-%Y")
    today = dt.datetime.now(BZ_TIMEZONE).date()
    df["data"] = pd.Timestamp(today)

    return df


def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    reorder_columns = [
        # "codigo_negociacao",
        "data",
        "titulo",
        "codigo_titulo",
        "vencimento",
        "negocios",
        "negocios_corretagem",
        "negocios_termo",
        "negocios_corretagem_termo",
        "quantidade",
        "quantidade_corretagem",
        "quantidade_termo",
        "quantidade_corretagem_termo",
        "financeiro",
        "financeiro_termo",
        "tx_ultima",
        "tx_min",
        "tx_media",
        "tx_max",
        "tx_ultima_termo",
        "tx_min_termo",
        "tx_media_termo",
        "tx_max_termo",
        "pu_min",
        "pu_medio",
        "pu_max",
        "pu_ultimo",
        "pu_ultimo_termo",
        "pu_min_termo",
        "pu_medio_termo",
        "pu_max_termo",
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


def tpf_intraday_trades() -> pd.DataFrame:
    """Fetches the intraday data from BCB and returns it as a DataFrame."""
    if not is_selic_open():
        logger.info("Market is closed. Returning empty DataFrame.")
        return pd.DataFrame()

    try:
        raw_text = _fetch_csv_from_url()
        # raw_text = test_file.read_text(encoding="iso-8859-15")
        cleaned_text = _clean_csv(raw_text)
        if not cleaned_text:
            logger.warning("No data found in the CSV file. Returning empty DataFrame.")
            return pd.DataFrame()
        df = _convert_csv_to_df(cleaned_text)
        df = _process_df(df)
        df = _reorder_columns(df)
        return df
    except Exception:
        logger.exception("Error fetching data from BCB. Returning empty DataFrame.")
        return pd.DataFrame()
