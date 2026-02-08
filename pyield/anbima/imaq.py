"""
HTML page example:
    Título Codigo Selic  Código ISIN Data de Vencimento Quantidade em Mercado (1.000 Títulos)    PU (R$) Valor de Mercado (R$ Mil)  Variação da Quantidade  (1.000 Títulos)        Status do Titulo
       LTN       100000 BRSTNCLTN863         01/10/2025                           115.870,772 997,241543               115.551.147                                    0,000 Participante Definitivo
       LTN       100000 BRSTNCLTN7U7         01/01/2026                           176.807,732 963,001853               170.266.174                                   -1,987 Participante Definitivo
       LTN       100000 BRSTNCLTN8B5         01/04/2026                           115.826,847 931,607124               107.905.116                                    0,000 Participante Definitivo
"""  # noqa

import datetime as dt
import logging
import re

import polars as pl
import polars.selectors as ps
import requests
from lxml.html import HTMLParser
from lxml.html import fromstring as html_fromstring

import pyield.converters as cv
from pyield.anbima.tpf import tpf_data
from pyield.types import DateLike, any_is_empty

logger = logging.getLogger(__name__)

IMA_URL = "https://www.anbima.com.br/informacoes/ima/ima-quantidade-mercado.asp"

COLUMN_MAP = {
    "Título": ("BondType", pl.String),
    "Codigo Selic": ("SelicCode", pl.Int64),
    "Código ISIN": ("ISIN", pl.String),
    "Data de Vencimento": ("MaturityDate", pl.String),
    "Quantidade em Mercado (1.000 Títulos)": ("MarketQuantity", pl.Float64),
    "PU (R$)": ("Price", pl.Float64),
    "Valor de Mercado (R$ Mil)": ("MarketValue", pl.Float64),
    "Variação da Quantidade (1.000 Títulos)": ("QuantityVariation", pl.Float64),
    "Status do Titulo": ("BondStatus", pl.String),
}

COLUMN_ALIASES = {col: alias for col, (alias, _) in COLUMN_MAP.items()}
DATA_SCHEMA = {alias: dtype for _, (alias, dtype) in COLUMN_MAP.items()}

INT_COLUMNS = [
    "MarketQuantity",
    "MarketValue",
    "QuantityVariation",
    "MarketDV01",
    "MarketDV01USD",
]

FINAL_COLUMN_ORDER = [
    "Date",
    "BondType",
    "MaturityDate",
    "SelicCode",
    "ISIN",
    "Price",
    "MarketQuantity",
    "MarketDV01",
    "MarketDV01USD",
    "MarketValue",
    "QuantityVariation",
    "BondStatus",
]


def _fetch_url_content(target_date: dt.date) -> bytes:
    target_date_str = target_date.strftime("%d/%m/%Y")
    payload = {
        "Tipo": "",
        "DataRef": "",
        "Pai": "ima",
        "Dt_Ref_Ver": "20250117",
        "Dt_Ref": f"{target_date_str}",
    }

    r = requests.post(IMA_URL, data=payload, timeout=10)
    r.raise_for_status()
    if "Não há dados disponíveis" in r.text:
        return b""
    return r.content


def _extract_reference_date(html_content: bytes) -> dt.date | None:
    """
    Extract reference date from HTML content.

    Returns:
        The reference date found in the HTML, or None if not found.
    """
    date_pattern = r"\b(\d{2}/\d{2}/\d{4})\b"
    match = re.search(date_pattern, html_content.decode("iso-8859-1"))

    if not match:
        return None

    date_string = match.group(1)
    return dt.datetime.strptime(date_string, "%d/%m/%Y").date()


def _normalize_column_name(text: str) -> str:
    """Normalize column header by removing line breaks and extra spaces."""
    return " ".join(text.strip().split())


def _parse_cell_value(text: str) -> str:
    """
    Parse cell value, converting Brazilian number format to standard.

    Brazilian format: 129.253,568 -> 129253.568
    Handles missing values (--) by returning empty string.
    """
    text = text.strip()

    if text == "--" or not text:
        return ""

    # Convert Brazilian number format
    if "," in text or "." in text:
        if any(c.isdigit() for c in text):
            text = text.replace(".", "")  # Remove thousands separator
            text = text.replace(",", ".")  # Replace decimal separator

    return text


def _parse_html_tables(html_content: bytes) -> pl.DataFrame:
    """Parse HTML tables using lxml e retorna DataFrame (colunas String).

    Extrai dados das tabelas aninhadas (com parent::td),
    converte formato numérico brasileiro e retorna DataFrame bruto.
    """
    html_content = html_content.replace(b"<br>", b" ").replace(b"<BR>", b" ")

    parser = HTMLParser(encoding="iso-8859-1")
    tree = html_fromstring(html_content, parser=parser)

    nested_tables = tree.xpath("//table[@width='100%'][parent::td]")

    all_data = []
    col_names = None

    for table in nested_tables:  # type: ignore[misc]
        headers = table.xpath(".//thead//th")
        if not col_names:
            col_names = [_normalize_column_name(h.text_content()) for h in headers]

        data_rows = table.xpath(".//tbody//tr[td]")
        for row in data_rows:
            cells = row.xpath(".//td")
            if len(cells) != len(col_names):
                continue
            all_data.append([_parse_cell_value(c.text_content()) for c in cells])

    if not all_data or not col_names:
        return pl.DataFrame()

    return pl.DataFrame(all_data, schema=col_names, orient="row")


def _process_df(df: pl.DataFrame, reference_date: dt.date) -> pl.DataFrame:
    """Renomeia, filtra, converte tipos e aplica transformações numéricas."""
    return (
        df.rename(COLUMN_ALIASES)
        # Strip whitespace e converte strings vazias em null
        .with_columns(ps.string().str.strip_chars().name.keep())
        .with_columns(
            pl.when(ps.string().str.len_chars() == 0)
            .then(None)
            .otherwise(ps.string())
            .name.keep()
        )
        .filter(
            pl.col("MaturityDate").is_not_null(),
            pl.col("BondType") != "Título",
        )
        .unique(subset="ISIN")
        .cast(DATA_SCHEMA)
        .with_columns(
            pl.col("MaturityDate").str.to_date(format="%d/%m/%Y"),
            pl.col("MarketQuantity") * 1000,
            pl.col("MarketValue") * 1000,
            pl.col("QuantityVariation") * 1000,
            Date=reference_date,
        )
        .sort("BondType", "MaturityDate")
    )


def _add_dv01(df: pl.DataFrame, reference_date: dt.date) -> pl.DataFrame:
    df_anbima = tpf_data(reference_date)
    keep_cols = ["ReferenceDate", "BondType", "MaturityDate", "DV01", "DV01USD"]
    df_anbima = df_anbima.select(keep_cols).rename({"ReferenceDate": "Date"})
    # Guard clause for missing columns
    if "DV01" not in df_anbima.columns or "DV01USD" not in df_anbima.columns:
        return df

    df = df.join(df_anbima, on=["Date", "BondType", "MaturityDate"], how="left")
    # Calcular os estoques
    df = df.with_columns(
        MarketDV01=pl.col("DV01") * pl.col("MarketQuantity"),
        MarketDV01USD=pl.col("DV01USD") * pl.col("MarketQuantity"),
    ).drop("DV01", "DV01USD")
    return df


def _finalize(df: pl.DataFrame) -> pl.DataFrame:
    """Converte colunas inteiras e reordena colunas para saída final."""
    return df.with_columns(pl.col(INT_COLUMNS).round(0).cast(pl.Int64)).select(
        FINAL_COLUMN_ORDER
    )


def imaq(date: DateLike) -> pl.DataFrame:
    """Consulta e processa dados de estoque IMA-Q da ANBIMA para uma data.

    Args:
        date: Data de referência. Apenas os últimos 5 dias úteis estão
            disponíveis; o mais recente é tipicamente 2 dias úteis atrás.

    Returns:
        DataFrame com dados processados. Em caso de erro retorna DataFrame
        vazio e registra log da exceção.

    Output Columns:
        * Date (Date): data de referência dos dados.
        * BondType (String): tipo do título (LTN, NTN-B, NTN-F, LFT, …).
        * MaturityDate (Date): data de vencimento do título.
        * SelicCode (Int64): código SELIC do título.
        * ISIN (String): código ISIN (International Securities Id Number).
        * Price (Float64): PU do título em R$.
        * MarketQuantity (Int64): quantidade em mercado (unidades).
        * MarketDV01 (Int64): DV01 do estoque em R$.
        * MarketDV01USD (Int64): DV01 do estoque em USD.
        * MarketValue (Int64): valor de mercado em R$.
        * QuantityVariation (Int64): variação diária da quantidade.
        * BondStatus (String): status do título.

    Notes:
        - Valores convertidos para unidades puras (ex: MarketQuantity × 1.000).
        - DV01 obtidos via cruzamento com tpf_data(); nulos se indisponível.

    Examples:
        >>> from pyield import bday
        >>> target_date = bday.offset(bday.last_business_day(), -2)
        >>> df = imaq(target_date)
        >>> df["Date"].first() == target_date
        True
    """
    if any_is_empty(date):
        logger.warning("No date provided. Returning empty DataFrame.")
        return pl.DataFrame()
    date = cv.convert_dates(date)
    date_str = date.strftime("%d/%m/%Y")
    try:
        url_content = _fetch_url_content(date)
        if not url_content:
            logger.warning(
                f"No data available for {date_str}. Returning an empty DataFrame."
            )
            return pl.DataFrame()

        # ✅ VALIDAÇÃO CRÍTICA EXPLÍCITA NO FLUXO PRINCIPAL
        reference_date = _extract_reference_date(url_content)

        if reference_date is None:
            raise ValueError(f"No reference date found in HTML for {date_str}")

        if reference_date != date:
            raise ValueError(
                f"Reference date mismatch: expected {date_str}, "
                f"got {reference_date.strftime('%d/%m/%Y')}"
            )

        df = _parse_html_tables(url_content)
        if df.is_empty():
            return pl.DataFrame()
        df = _process_df(df, date)
        df = _add_dv01(df, date)
        return _finalize(df)
    except Exception:  # Erro inesperado
        msg = f"Error fetching IMA for {date_str}. Returning empty DataFrame."
        logger.exception(msg)
        return pl.DataFrame()
