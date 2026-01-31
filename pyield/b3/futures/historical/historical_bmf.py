import datetime as dt
import logging

import polars as pl
import polars.selectors as cs
import requests
from lxml import html

from pyield import bday
from pyield.b3.common import add_expiration_date
from pyield.fwd import forwards
from pyield.retry import default_retry

logger = logging.getLogger(__name__)
COUNT_CONVENTIONS = {"DAP": 252, "DI1": 252, "DDI": 360}
BDAYS_PER_YEAR = 252
CDAYS_PER_YEAR = 360
# "Nome Original": ("Tradução", Tipo)
COLUMN_MAP = {
    "VENCTO": ("ExpirationCode", pl.Utf8),
    "CONTR. ABERT.(1)": ("OpenContracts", pl.Int64),
    "CONTR. FECH.(2)": ("OpenContractsEndSession", pl.Int64),
    "NÚM. NEGOC.": ("TradeCount", pl.Int64),
    "CONTR. NEGOC.": ("TradeVolume", pl.Int64),
    "VOL.": ("FinancialVolume", pl.Int64),
    "AJUSTE ANTER. (3)": ("PrevSettlementPrice", pl.Float64),
    "AJUSTE CORRIG. (4)": ("AdjSettlementPrice", pl.Float64),
    "PREÇO ABERTU.": ("OpenRate", pl.Float64),
    "PREÇO MÍN.": ("MinRate", pl.Float64),
    "PREÇO MÁX.": ("MaxRate", pl.Float64),
    "PREÇO MÉD.": ("AvgRate", pl.Float64),
    "ÚLT. PREÇO": ("CloseRate", pl.Float64),
    "AJUSTE\n       DE REF.": ("SettlementRate", pl.Float64),  # Somente FRC
    "AJUSTE": ("SettlementPrice", pl.Float64),
    "VAR. PTOS.": ("PointsVariation", pl.Float64),
    "ÚLT.OF. COMPRA": ("CloseAskRate", pl.Float64),
    "ÚLT.OF. VENDA": ("CloseBidRate", pl.Float64),
}
COLUMN_NAMES = list(COLUMN_MAP.keys())
COLUMN_TYPES = {COLUMN_MAP[c][0]: COLUMN_MAP[c][1] for c in COLUMN_MAP}
COLUMN_RENAME = {c: COLUMN_MAP[c][0] for c in COLUMN_MAP}

OLD_MONTH_CODES = {
    "JAN": 1,
    "FEV": 2,
    "MAR": 3,
    "ABR": 4,
    "MAI": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SET": 9,
    "OUT": 10,
    "NOV": 11,
    "DEZ": 12,
}

OUTPUT_COLUMNS = [
    "TradeDate",
    "TickerSymbol",
    "ExpirationDate",
    "BDaysToExp",
    "DaysToExp",
    "OpenContracts",
    "TradeCount",
    "TradeVolume",
    "FinancialVolume",
    "DV01",
    # "PrevSettlementPrice", # Pode ser inferido a partir da base histórica
    # "AdjSettlementPrice", # Aparentemente é igual ao SettlementPrice
    "SettlementPrice",
    # "PointsVariation", # Pode ser inferido a partir da base histórica
    "OpenRate",
    "MinRate",
    "AvgRate",
    "MaxRate",
    "CloseAskRate",
    "CloseBidRate",
    "CloseRate",
    "SettlementRate",
    "ForwardRate",
]


def _calculate_legacy_expiration_date(
    date: dt.date, expiration_code: str
) -> dt.date | None:
    """
    Converts an old DI contract code into its expiration date (valid until 21-05-2006).

    Args:
        date: The trade date for which the contract code is valid.
        expiration_code: An old DI expiration code (e.g., "JAN3").

    Returns:
        The expiration date, or None if invalid.

    Examples:
        >>> _calculate_legacy_expiration_date(dt.date(2001, 5, 21), "JAN3")
        datetime.date(2003, 1, 2)
    """
    try:
        month = OLD_MONTH_CODES[expiration_code[:3]]
        year_digit = int(expiration_code[-1])
        year = date.year // 10 * 10 + year_digit

        # Se o ano calculado for anterior ao ano de referência, avança uma década
        if year < date.year:
            year += 10

        expiration_date = dt.date(year, month, 1)
        return bday.offset(dates=expiration_date, offset=0)
    except (KeyError, ValueError):
        return None


def _convert_prices_to_rates(
    prices: pl.Series,
    days_to_expiration: pl.Series,
    count_convention: int,
) -> pl.Series:
    """Converts DI futures prices to rates using Polars."""
    if count_convention == CDAYS_PER_YEAR:
        rates = (100_000 / prices - 1) * (CDAYS_PER_YEAR / days_to_expiration)
    else:  # 252
        rates = (100_000 / prices) ** (BDAYS_PER_YEAR / days_to_expiration) - 1
    return rates.round(5)


@default_retry
def _fetch_html_data(date: dt.date, contract_code: str) -> str:
    """Fetches HTML data from B3 for a given date and contract code."""
    url_date = date.strftime("%d/%m/%Y")
    url_base = "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp"
    params = {"Data": url_date, "Mercadoria": contract_code, "XLS": "true"}
    r = requests.get(url_base, params=params, timeout=10)
    r.raise_for_status()
    r.encoding = "iso-8859-1"
    return r.text


def _parse_html_lxml(html_text: str) -> pl.DataFrame:
    """Parses HTML table using lxml and returns a DataFrame with string columns."""
    if not html_text:
        return pl.DataFrame()

    tree = html.fromstring(html_text)

    # 1. Extrair o header do HTML
    header_cells = tree.xpath(
        '//tr[@class="tabelaSubTitulo"]//th | //tr[@class="tabelaSubTitulo"]//td'
    )
    col_names = [cell.text_content().strip() for cell in header_cells]  # type: ignore
    if "VENCTO" not in col_names:
        return pl.DataFrame()

    # 2. Extrair as linhas de dados
    rows = tree.xpath('//tr[@class="tabelaConteudo1" or @class="tabelaConteudo2"]')

    data = []
    for row in rows:  # type: ignore
        cells = row.xpath(".//td")
        clean_cells = [cell.text_content().strip() for cell in cells]
        if len(clean_cells) == len(col_names):
            data.append(clean_cells)

    return pl.DataFrame(data, schema=col_names, orient="row")


def _clean_string_values(df: pl.DataFrame) -> pl.DataFrame:
    """Remove all thousands separators and adjust decimal separators."""
    if "PointsVariation" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("PointsVariation").str.ends_with("-"))
            .then("-" + pl.col("PointsVariation").str.replace("-", "", literal=True))
            .otherwise(pl.col("PointsVariation").str.replace("+", "", literal=True))
            .alias("PointsVariation")
        )

    df = df.select(
        pl.all()
        .str.strip_chars()
        .str.replace_all(".", "", literal=True)
        .str.replace(",", ".")
        .replace("-", "")
    )
    return df


def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Casts columns to their appropriate data types."""
    column_types = {k: v for k, v in COLUMN_TYPES.items() if k in df.columns}
    return df.cast(column_types, strict=False)


def _add_expiration_dates(
    df: pl.DataFrame, date: dt.date, contract_code: str
) -> pl.DataFrame:
    """Adds ExpirationDate, DaysToExp, and BDaysToExp columns."""
    df = df.with_columns(
        TradeDate=date,
        TickerSymbol=contract_code + pl.col("ExpirationCode"),
    )

    if date < dt.date(2006, 5, 22):
        # Before 22-05-2006, use old expiration date logic
        exp_dates = [
            _calculate_legacy_expiration_date(date, exp_code)
            for exp_code in df["ExpirationCode"]
        ]
        df = df.with_columns(pl.Series("ExpirationDate", exp_dates))
    else:
        df = add_expiration_date(df, contract_code, "TickerSymbol")

    df = (
        df.with_columns(
            BDaysToExp=bday.count(date, df["ExpirationDate"]),
            DaysToExp=(pl.col("ExpirationDate") - pl.col("TradeDate")).dt.total_days(),
        ).filter(pl.col("DaysToExp") > 0)  # Filter out expired contracts
    )

    return df


def _convert_zeros_to_null(df: pl.DataFrame) -> pl.DataFrame:
    """Converts zero values to null in rate and price columns."""
    return df.with_columns(
        (cs.contains("Rate") | cs.contains("Price")).replace(0, None)
    )


def _adjust_legacy_di1_rates(df: pl.DataFrame, rate_cols: list) -> pl.DataFrame:
    """Adjusts legacy DI1 contract pricing (pre-2002) converting prices -> rates."""
    for col in rate_cols:
        rate_col = _convert_prices_to_rates(df[col], df["BDaysToExp"], BDAYS_PER_YEAR)
        df = df.with_columns(rate_col.alias(col))

    # For older contracts, min/max rates are inverted
    if {"MinRate", "MaxRate"}.issubset(set(rate_cols)):
        df = df.with_columns(
            MinRate=pl.col("MaxRate"),
            MaxRate=pl.col("MinRate"),
        )
    return df


def _transform_rates(
    df: pl.DataFrame, date: dt.date, contract_code: str
) -> pl.DataFrame:
    """Transforms rate columns: divides by 100 or converts from prices."""
    rate_cols = [c for c in df.columns if "Rate" in c]

    switch_date = dt.date(2002, 1, 17)
    if date <= switch_date and contract_code == "DI1":
        df = _adjust_legacy_di1_rates(df, rate_cols)
    else:
        if contract_code in {"FRC", "FRO"}:
            rate_cols.append("PointsVariation")
        df = df.with_columns(pl.col(rate_cols).truediv(100).round(5))

    return df


def _add_derived_columns(df: pl.DataFrame, contract_code: str) -> pl.DataFrame:
    """Adds SettlementRate, DV01, and ForwardRate columns."""
    count_conv = COUNT_CONVENTIONS.get(contract_code)

    # Settlement Rate
    if count_conv in {252, 360} and "SettlementPrice" in df.columns:
        n_days = df["BDaysToExp"] if count_conv == BDAYS_PER_YEAR else df["DaysToExp"]
        df = df.with_columns(
            SettlementRate=_convert_prices_to_rates(
                df["SettlementPrice"], n_days, count_conv
            )
        )

    # DV01 (DI1 only)
    has_settlement_cols = {"SettlementRate", "SettlementPrice"}.issubset(df.columns)
    if contract_code == "DI1" and has_settlement_cols:
        duration = pl.col("BDaysToExp") / 252
        m_duration = duration / (1 + pl.col("SettlementRate"))
        df = df.with_columns(DV01=0.0001 * m_duration * pl.col("SettlementPrice"))

    # Forward Rate
    if contract_code in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        df = df.with_columns(
            ForwardRate=forwards(df["BDaysToExp"], df["SettlementRate"])
        )

    return df


def fetch_old_historical_df(date: dt.date, contract_code: str) -> pl.DataFrame:
    """
    Fetches the futures data for a given date from B3.

    This function fetches and processes the futures data from B3 for a specific
    trade date. It's the primary external interface for accessing futures data.

    Args:
        date: The trade date to fetch the futures data.
        contract_code: The asset code to fetch the futures data.

    Returns:
        Processed futures data as a Polars DataFrame. If no data is found,
        returns an empty DataFrame.
    """
    html_text = _fetch_html_data(date, contract_code)
    df = _parse_html_lxml(html_text)
    if df.is_empty():
        return pl.DataFrame()

    df = df.rename(COLUMN_RENAME, strict=False)
    df = _clean_string_values(df)
    df = cast_columns(df)
    df = _add_expiration_dates(df, date, contract_code)
    df = _convert_zeros_to_null(df)
    df = _transform_rates(df, date, contract_code)
    df = _add_derived_columns(df, contract_code)

    # Select and reorder output columns
    return df.select([c for c in OUTPUT_COLUMNS if c in df.columns])
