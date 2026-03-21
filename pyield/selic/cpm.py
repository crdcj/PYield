"""
CPM — B3 COPOM Digital Option contract data.

Ticker format: CPM{month_code}{year2}{C|P}{strike_6digits}
Example:       CPMZ25C099500
               ^^^            → prefix "CPM"
                  ^           → month code "Z" = December
                   ^^         → year "25" = 2025
                     ^        → option type "C" = call
                      ^^^^^^  → strike "099500" → 99.500

Strike interpretation (Selic Meta context):
    strike_float = int(strike_6digits) / 1000   # e.g. 99.500
    change_bps   = round((strike_float - 100) * 100)  # e.g. -50 bps

Month codes (B3 standard futures convention):
    F=1, G=2, H=3, J=4, K=5, M=6,
    N=7, Q=8, U=9, V=10, X=11, Z=12

Implementation note
-------------------
CPM options expire on the first business day AFTER the COPOM meeting
ends (ExpiryDate), not on the first business day of the meeting month.
This module bypasses the generic B3 pipeline (which applies a 6-char
ticker filter and a DaysToExp > 0 filter, both incorrect for CPM) and
calls the lower-level price-report helpers directly.

ExpiryDate and MeetingEndDate are resolved by joining against the COPOM
calendar (bc.copom.calendar()) on meeting month + year extracted from
the ticker.  The join is a left join so CPM rows are never dropped even
if the calendar has gaps for very recently announced future meetings.
"""

import datetime as dt
import logging

import polars as pl
import requests

import pyield._internal.converters as cv
from pyield import bday
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike
from pyield.b3._validar_pregao import data_negociacao_valida
from pyield.b3.price_report import (
    _baixar_zip_url,
    _converter_para_df,
    _extrair_xml_de_zip,
    _mapa_renomeacao_colunas,
    _parsear_xml_registros,
)

logger = logging.getLogger(__name__)

# B3 futures month code → calendar month integer (same mapping as common.py)
_MONTH_CODES: dict[str, int] = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}

# Polars-compatible replacement map (str → str, cast later)
_MONTH_CODE_STR: dict[str, str] = {k: str(v) for k, v in _MONTH_CODES.items()}
_CPM_TICKER_LENGTH = 13


def _empty_schema() -> pl.DataFrame:
    """Return an empty DataFrame with the canonical CPM output schema."""
    return pl.DataFrame(
        schema={
            "TradeDate": pl.Date,
            "TickerSymbol": pl.String,
            "MeetingEndDate": pl.Date,
            "ExpiryDate": pl.Date,
            "OptionType": pl.String,
            "StrikeChangeBps": pl.Int32,
            "SettlementPrice": pl.Float64,
            "BDaysToExp": pl.Int32,
        }
    )


# ---------------------------------------------------------------------------
# B3 CSV endpoint helpers (inlined from the former historical_b3 module)
# ---------------------------------------------------------------------------

# Column config for the B3 consolidated derivatives CSV.
_CSV_CONFIG_COLUNAS = {
    "Instrumento financeiro": (pl.String, "TickerSymbol"),
    "Preço de referência": (pl.Float64, "ReferencePrice"),
}
_CSV_ESQUEMA = {k: v[0] for k, v in _CSV_CONFIG_COLUNAS.items()}
_CSV_MAPA_RENOMEACAO = {k: v[1] for k, v in _CSV_CONFIG_COLUNAS.items()}


@retry_padrao
def _buscar_csv(data: dt.date) -> bytes:
    """Busca o CSV diário de derivativos consolidados na B3."""
    url = "https://arquivos.b3.com.br/bdi/table/export/csv"
    parametros = {"lang": "pt-BR"}
    data_str = data.strftime("%Y-%m-%d")
    carga = {
        "Name": "ConsolidatedTradesDerivatives",
        "Date": data_str,
        "FinalDate": data_str,
        "ClientId": "",
        "Filters": {},
    }
    cabecalhos = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa: E501
        "Accept": "application/json, text/plain, */*",
    }
    resposta = requests.post(
        url, params=parametros, json=carga, headers=cabecalhos, timeout=(5, 30)
    )
    resposta.raise_for_status()
    return resposta.content


def _parsear_df_bruto(csv_bytes: bytes) -> pl.DataFrame:
    """Lê o CSV bruto em um DataFrame Polars."""
    return pl.read_csv(
        csv_bytes.replace(b".", b""),
        separator=";",
        skip_lines=2,
        null_values=["-"],
        decimal_comma=True,
        schema_overrides=_CSV_ESQUEMA,
        encoding="utf-8-sig",
    )


def _preprocessar_df(df: pl.DataFrame, codigo_contrato: str) -> pl.DataFrame:
    """Renomeia e filtra o DataFrame para o contrato desejado."""
    return df.rename(_CSV_MAPA_RENOMEACAO, strict=False).filter(
        pl.col("TickerSymbol").str.starts_with(codigo_contrato),
        pl.col("TickerSymbol").str.len_chars().is_in([6, 13]),
    )


def _fetch_settlement_prices(trade_date: dt.date) -> pl.DataFrame:
    """
    Fetch CPM settlement prices from the B3 CSV endpoint.

    Returns a DataFrame with columns (TickerSymbol, SettlementPrice), where
    SettlementPrice is the B3 "Preço de Referência" — the official contract
    price shown on the B3 dashboard ("Probabilidades da Taxa Selic Meta").

    Returns an empty DataFrame (correct schema) if the CSV endpoint is
    unavailable or has no data for the date (the endpoint retains ~1 month
    of history).
    """
    empty = pl.DataFrame(
        schema={"TickerSymbol": pl.String, "SettlementPrice": pl.Float64}
    )
    try:
        csv_bytes = _buscar_csv(trade_date)
        df = _parsear_df_bruto(csv_bytes)
        df = _preprocessar_df(df, "CPM")
        if "ReferencePrice" not in df.columns:
            return empty
        return df.select(
            "TickerSymbol", pl.col("ReferencePrice").alias("SettlementPrice")
        )
    except Exception:
        logger.debug("CPM: settlement prices unavailable from CSV for %s.", trade_date)
        return empty


# ---------------------------------------------------------------------------
# Ticker parsing
# ---------------------------------------------------------------------------


def _parse_ticker(ticker: str) -> tuple[int, int, str, float, int]:
    """
    Parse a CPM ticker string into its components.

    Returns (month, year, option_type, strike_float, change_bps).
    Raises ValueError for malformed tickers.

    Examples
    --------
    >>> _parse_ticker("CPMZ25C099500")
    (12, 2025, 'call', 99.5, -50)

    >>> _parse_ticker("CPMF25C100750")
    (1, 2025, 'call', 100.75, 75)

    >>> _parse_ticker("CPMH25P100000")
    (3, 2025, 'put', 100.0, 0)
    """
    if len(ticker) != _CPM_TICKER_LENGTH or not ticker.startswith("CPM"):
        raise ValueError(f"Invalid CPM ticker: {ticker!r}")

    month_code = ticker[3]
    year_str = ticker[4:6]
    opt_char = ticker[6]
    strike_str = ticker[7:13]

    month = _MONTH_CODES.get(month_code)
    if month is None:
        raise ValueError(f"Unknown month code {month_code!r} in ticker {ticker!r}")

    year = int(year_str) + 2000

    if opt_char == "C":
        option_type = "call"
    elif opt_char == "P":
        option_type = "put"
    else:
        raise ValueError(f"Unknown option type {opt_char!r} in ticker {ticker!r}")

    strike = int(strike_str) / 1000
    change_bps = round((strike - 100) * 100)

    return month, year, option_type, strike, change_bps


def data(date: DateLike) -> pl.DataFrame:
    """
    Fetch B3 end-of-day CPM contract data for a given trade date.

    Parameters
    ----------
    date : DateLike
        Trade date. Accepts DD-MM-YYYY, YYYY-MM-DD, datetime.date, etc.

    Returns
    -------
    pl.DataFrame
        Columns:
            TradeDate      : date
            TickerSymbol   : str
            MeetingEndDate : date   actual COPOM meeting end date (from BCB calendar)
            ExpiryDate     : date   next business day after MeetingEndDate
                                    (= B3 CPM contract settlement date)
            OptionType     : str    "call" or "put"
            StrikeChangeBps: int    change in bps vs 100.000 strike
            SettlementPrice: float  B3 official "Preço de Referência" from the
                                    CSV endpoint, in points (0–100).  This is
                                    the price shown on the B3 dashboard
                                    ("Probabilidades da Taxa Selic Meta").
                                    Null for older dates (> ~1 month) where
                                    the CSV endpoint is unavailable.
            BDaysToExp     : int    business days from TradeDate to ExpiryDate

        Returns empty DataFrame with correct schema if no CPM data
        exists for the requested date (weekend, holiday, etc.).

    Examples
    --------
    >>> import pyield as yd
    >>> df = yd.selic.cpm.data("29-01-2025")
    >>> df.is_empty() or set(df.schema.keys()) >= {
    ...     "TradeDate",
    ...     "TickerSymbol",
    ...     "MeetingEndDate",
    ...     "ExpiryDate",
    ...     "OptionType",
    ...     "StrikeChangeBps",
    ...     "SettlementPrice",
    ... }
    True
    """
    trade_date = cv.converter_datas(date) if date is not None else None
    if trade_date is None:
        return _empty_schema()

    if not data_negociacao_valida(trade_date):
        logger.warning(
            "Data %s inválida para CPM. Retornando DataFrame vazio.", trade_date
        )
        return _empty_schema()

    try:
        zip_data = _baixar_zip_url(trade_date, relatorio_completo=False)
        if not zip_data:
            return _empty_schema()
        xml_bytes = _extrair_xml_de_zip(zip_data)
        records = _parsear_xml_registros(xml_bytes, "CPM")
    except Exception:
        logger.exception("CPM: falha ao baixar SPR para %s.", trade_date)
        return _empty_schema()

    if not records:
        return _empty_schema()

    df = _converter_para_df(records)

    mapa = _mapa_renomeacao_colunas()
    df = df.rename(mapa, strict=False)
    df = df.with_columns(TradeDate=trade_date)

    # Parse option type (ticker[6]) and strike change (ticker[7:13])
    # entirely with Polars string expressions — no Python loops over rows.
    df = df.with_columns(
        OptionType=pl.col("TickerSymbol")
        .str.slice(6, 1)
        .replace({"C": "call", "P": "put"}),
        # strike_int / 10 − 10_000  ≡  round((strike_int/1000 − 100) * 100)
        # because CPM strikes are multiples of 250 (25 bp increments).
        StrikeChangeBps=(
            pl.col("TickerSymbol")
            .str.slice(7, 6)
            .cast(pl.Int64, strict=False)
            .floordiv(10)
            .sub(10_000)
            .cast(pl.Int32)
        ),
        # Meeting month and year extracted from ticker positions 3 and 4-5.
        # Used as join keys against the COPOM calendar.
        _meeting_month=(
            pl.col("TickerSymbol")
            .str.slice(3, 1)
            .replace(_MONTH_CODE_STR)
            .cast(pl.Int32, strict=False)
        ),
        _meeting_year=(
            pl.col("TickerSymbol")
            .str.slice(4, 2)
            .cast(pl.Int32, strict=False)
            .add(2000)
        ),
    )

    # Join with COPOM calendar to get MeetingEndDate and the correct ExpiryDate.
    # Import is deferred to avoid a module-level circular dependency risk.
    from pyield.bc import copom  # noqa: PLC0415

    cal = copom.calendar().select(
        _meeting_month=pl.col("EndDate").dt.month().cast(pl.Int32),
        _meeting_year=pl.col("EndDate").dt.year().cast(pl.Int32),
        MeetingEndDate=pl.col("EndDate"),
        ExpiryDate=pl.col("ExpiryDate"),
    )

    df = df.join(cal, on=["_meeting_month", "_meeting_year"], how="left").drop(
        "_meeting_month", "_meeting_year"
    )

    # BDaysToExp: business days from TradeDate (inclusive) to ExpiryDate (exclusive).
    # Vectorized via count_expr — consistent with the DI1 pipeline convention.
    df = df.with_columns(
        BDaysToExp=bday.count_expr("TradeDate", "ExpiryDate").cast(pl.Int32)
    )

    # SettlementPrice: B3 "Preço de Referência" from the CSV endpoint.
    # The SPR XML for option contracts lacks this field (no RfPric tag);
    # the XML is only used above to obtain the contract list (tickers).
    sett_prices = _fetch_settlement_prices(trade_date)
    if not sett_prices.is_empty():
        df = df.join(sett_prices, on="TickerSymbol", how="left")
    else:
        df = df.with_columns(SettlementPrice=pl.lit(None, dtype=pl.Float64))

    return df.select(
        pl.col("TradeDate"),
        pl.col("TickerSymbol"),
        pl.col("MeetingEndDate"),
        pl.col("ExpiryDate"),
        pl.col("OptionType"),
        pl.col("StrikeChangeBps"),
        pl.col("SettlementPrice"),
        pl.col("BDaysToExp"),
    ).sort("ExpiryDate", "StrikeChangeBps")
