import datetime as dt
import io
import logging
import zipfile
from pathlib import Path
from typing import Literal

import polars as pl
import polars.selectors as cs
import requests
from lxml import etree
from lxml.etree import _Element

import pyield.converters as cv
from pyield import bday
from pyield.b3.common import add_expiration_date
from pyield.fwd import forwards
from pyield.retry import DataNotAvailableError, default_retry
from pyield.types import DateLike, has_nullable_args

logger = logging.getLogger(__name__)

# --- Contract Configuration ---
# Rate-based contracts (use "Rate" suffix in columns like "OpenRate", "CloseRate")
# Price-based contracts use "Price" suffix (like "OpenPrice", "ClosePrice")
RATE_CONTRACTS = {"DI1", "DAP", "DDI", "FRC", "FRO"}

# --- XML Processing Constants ---
B3_NAMESPACE = "urn:bvmf.217.01.xsd"
NAMESPACES = {"ns": B3_NAMESPACE}
# Minimum valid price report ZIP is ~2KB; 1KB threshold detects "no data" stub files
MIN_ZIP_SIZE_BYTES = 1024
# B3 ticker format: AAAnYY (e.g., DI1F26 = DI1 contract, January 2026)
TICKER_LENGTH = 6
TICKER_XPATH_TEMPLATE = '//ns:TckrSymb[starts-with(text(), "{asset_code}")]'
TRADE_DATE_XPATH = ".//ns:TradDt/ns:Dt"
FIN_INSTRM_ATTRBTS_XPATH = ".//ns:FinInstrmAttrbts"

# --- Column Mappings ---

# 1. Fixed columns: Destination name is always the same regardless of asset
# Format: {XML_Name: (New_Name, DataType)}
BASE_MAPPING = {
    "TradDt": ("TradeDate", pl.Date),
    "TckrSymb": ("TickerSymbol", pl.String),
    "OpnIntrst": ("OpenContracts", pl.Int64),
    "RglrTxsQty": ("TradeCount", pl.Int64),
    "FinInstrmQty": ("TradeVolume", pl.Int64),
    "NtlFinVol": ("FinancialVolume", pl.Float64),
    "AdjstdQt": ("SettlementPrice", pl.Float64),  # Settlement price (PU - Unit Price)
    "AdjstdQtTax": (
        "SettlementRate",
        pl.Float64,
    ),  # Settlement rate (common in DI contracts)
    "RglrTraddCtrcts": ("RegularTradedContracts", pl.Int64),
    "NtlRglrVol": ("NationalRegularVolume", pl.Float64),
    "IntlRglrVol": ("InternationalRegularVolume", pl.Float64),
    "OscnPctg": ("OscillationPercentage", pl.Float64),
    "VartnPts": ("VariationPoints", pl.Float64),
    "AdjstdValCtrct": ("AdjustedValueContract", pl.Float64),
    "MktDataStrmId": ("MarketDataStreamId", pl.String),
    "IntlFinVol": ("InternationalFinancialVolume", pl.Float64),
    "AdjstdQtStin": ("AdjustedQuotationIndicator", pl.String),
    "PrvsAdjstdQt": ("PreviousAdjustedQuotation", pl.Float64),
    "PrvsAdjstdQtTax": ("PreviousAdjustedRate", pl.Float64),
    "PrvsAdjstdQtStin": ("PreviousAdjustedIndicator", pl.String),
}

# 2. Variable columns: Destination name depends on suffix (Rate or Price)
# Format: {XML_Name: (Prefix, DataType)}
VARIABLE_MAPPING = {
    "MinTradLmt": ("MinLimit", pl.Float64),
    "MaxTradLmt": ("MaxLimit", pl.Float64),
    "BestAskPric": ("BestAsk", pl.Float64),
    "BestBidPric": ("BestBid", pl.Float64),
    "FrstPric": ("Open", pl.Float64),
    "MinPric": ("Min", pl.Float64),
    "TradAvrgPric": ("Avg", pl.Float64),
    "MaxPric": ("Max", pl.Float64),
    "LastPric": ("Close", pl.Float64),
}

# Aggregate all types for initial casting (using original XML names)
ALL_XML_TYPES = {k: v[1] for k, v in BASE_MAPPING.items()}
ALL_XML_TYPES.update({k: v[1] for k, v in VARIABLE_MAPPING.items()})

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
    "SettlementPrice",
    # Columns that can be Rate or Price depending on contract type
    "MinLimitRate",
    "MinLimitPrice",
    "MaxLimitRate",
    "MaxLimitPrice",
    "BestBidRate",
    "BestBidPrice",
    "BestAskRate",
    "BestAskPrice",
    "OpenRate",
    "OpenPrice",
    "MinRate",
    "MinPrice",
    "AvgRate",
    "AvgPrice",
    "MaxRate",
    "MaxPrice",
    "CloseRate",
    "ClosePrice",
    "SettlementRate",
    "ForwardRate",
    # Other fields
    "RegularTradedContracts",
    "NationalRegularVolume",
    "InternationalRegularVolume",
    "OscillationPercentage",
    "VariationPoints",
    "AdjustedValueContract",
]


def _get_column_rename_map(contract_code: str) -> dict[str, str]:
    """
    Constrói o dicionário de renomeação dinamicamente baseado no contrato.
    Retorna: {XML_Name: New_Name}
    """
    # 1. Determina o sufixo (Rate ou Price)
    suffix = "Rate" if contract_code in RATE_CONTRACTS else "Price"

    # 2. Mapeamento Base (Fixo)
    rename_map = {k: v[0] for k, v in BASE_MAPPING.items()}

    # 3. Mapeamento Variável (Com Sufixo)
    # Ex: FrstPric -> OpenRate (se DI1) ou OpenPrice (se DOL)
    for xml_col, (prefix, _) in VARIABLE_MAPPING.items():
        rename_map[xml_col] = f"{prefix}{suffix}"

    return rename_map


def _fetch_zip_from_file(file_path: Path) -> bytes:
    if not isinstance(file_path, Path):
        raise ValueError("A file path must be provided.")
    if not file_path.exists():
        raise FileNotFoundError(f"No file found at {file_path}.")
    return file_path.read_bytes()


@default_retry
def _fetch_zip_from_url(date: dt.date, source_type: str) -> bytes:
    date_str = date.strftime("%y%m%d")
    if source_type == "PR":
        url = f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{date_str}.zip"
    elif source_type == "SPR":
        url = (
            f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRD{date_str}.zip"
        )
    else:
        raise ValueError("Invalid source type. Must be either 'PR' or 'SPR'.")

    response = requests.get(url, timeout=(5, 30))
    response.raise_for_status()

    if len(response.content) < MIN_ZIP_SIZE_BYTES:
        date_str_formatted = date.strftime("%Y-%m-%d")
        raise DataNotAvailableError(f"No data available for date {date_str_formatted}.")
    return response.content


def _extract_xml_from_nested_zip(zip_data: bytes) -> bytes:
    zip_file = io.BytesIO(zip_data)
    with zipfile.ZipFile(zip_file, "r") as outer_zip:
        outer_files = outer_zip.namelist()
        if not outer_files:
            raise ValueError("Outer ZIP file is empty")
        outer_file_name = outer_files[0]
        outer_file_content = outer_zip.read(outer_file_name)
    outer_file = io.BytesIO(outer_file_content)

    with zipfile.ZipFile(outer_file, "r") as inner_zip:
        filenames = inner_zip.namelist()
        xml_filenames = [name for name in filenames if name.endswith(".xml")]
        if not xml_filenames:
            raise ValueError("No XML files found in nested ZIP")
        xml_filenames.sort()
        inner_file_content = inner_zip.read(xml_filenames[-1])
    return inner_file_content


def _extract_contract_data(ticker: _Element) -> dict | None:
    if ticker.text is None or len(ticker.text) != TICKER_LENGTH:
        return None
    parent = ticker.getparent()
    if parent is None:
        return None
    price_report = parent.getparent()
    if price_report is None:
        return None
    date_elem = price_report.find(TRADE_DATE_XPATH, NAMESPACES)
    if date_elem is None:
        return None

    ticker_data = {"TradDt": date_elem.text, "TckrSymb": ticker.text}
    fin_instrm_attrbts = price_report.find(FIN_INSTRM_ATTRBTS_XPATH, NAMESPACES)
    if fin_instrm_attrbts is None:
        return None

    for attr in fin_instrm_attrbts:
        tag_name = etree.QName(attr).localname
        ticker_data[tag_name] = attr.text

    return ticker_data


def _parse_xml_to_records(xml_bytes: bytes, asset_code: str) -> list[dict]:
    parser = etree.XMLParser(
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        recover=True,
        resolve_entities=False,
        no_network=True,
        load_dtd=False,
    )
    xml_file = io.BytesIO(xml_bytes)
    tree = etree.parse(xml_file, parser=parser)
    path = TICKER_XPATH_TEMPLATE.format(asset_code=asset_code)
    tickers = tree.xpath(path, namespaces=NAMESPACES)

    if not tickers or not isinstance(tickers, list):
        return []

    records = []
    for ticker in tickers:
        if not isinstance(ticker, etree._Element):
            continue
        contract_data = _extract_contract_data(ticker)
        if contract_data is not None:
            records.append(contract_data)
    return records


def _convert_to_dataframe(records: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(records)
    # Casting usa os nomes originais do XML, que são constantes
    column_types = {k: v for k, v in ALL_XML_TYPES.items() if k in df.columns}
    return df.cast(column_types, strict=False)  # type: ignore


def _fill_zero_columns(df: pl.DataFrame) -> pl.DataFrame:
    zero_fill_cols = ["OpenContracts", "TradeCount", "TradeVolume", "FinancialVolume"]
    # Fill null values with 0 for volume/contract columns. Uses already-renamed
    # column names (applied by process_zip_file before calling this function).
    current_cols = set(df.columns)
    cols_to_fill = [c for c in zero_fill_cols if c in current_cols]

    if cols_to_fill:
        df = df.with_columns(pl.col(cols_to_fill).fill_null(0))
    return df


def _process_dataframe(df: pl.DataFrame, contract_code: str) -> pl.DataFrame:
    # 1. Add date-based metrics
    df = df.with_columns(
        BDaysToExp=bday.count(df["TradeDate"], df["ExpirationDate"]),
        DaysToExp=(pl.col("ExpirationDate") - pl.col("TradeDate")).dt.total_days(),
    )

    # 2. Normalize rates (divide by 100)
    # Selects only columns containing "Rate". Since we renamed correctly
    # (OpenPrice for DOL vs OpenRate for DI1), cs.contains("Rate") ignores
    # price columns and only affects rate columns.
    df = df.with_columns(cs.contains("Rate").truediv(100).round(5))

    # 3. Contract-specific derived columns
    if contract_code == "DI1":
        # DV01 requires both SettlementRate and SettlementPrice
        if "SettlementRate" in df.columns and "SettlementPrice" in df.columns:
            byears = pl.col("BDaysToExp") / 252
            mduration = byears / (1 + pl.col("SettlementRate"))
            df = df.with_columns(DV01=0.0001 * mduration * pl.col("SettlementPrice"))

    if contract_code in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        forward_rates = forwards(bdays=df["BDaysToExp"], rates=df["SettlementRate"])
        df = df.with_columns(ForwardRate=forward_rates)

    column_order = [col for col in OUTPUT_COLUMNS if col in df.columns]
    return df.select(column_order).filter(pl.col("DaysToExp") > 0)


def process_zip_file(
    zip_data: bytes, contract_code: str, source_type: str = "SPR"
) -> pl.DataFrame:
    if not zip_data:
        logger.warning("Empty XML zip file.")
        return pl.DataFrame()

    xml_bytes = _extract_xml_from_nested_zip(zip_data)
    records = _parse_xml_to_records(xml_bytes, contract_code)

    if not records:
        return pl.DataFrame()

    df = _convert_to_dataframe(records)

    # Apply dynamic column renaming based on contract type
    # 1. Generate correct rename map (Rate or Price suffix)
    rename_map = _get_column_rename_map(contract_code)

    # 2. Apply renaming (once only)
    df = df.rename(rename_map, strict=False)

    df = _fill_zero_columns(df)  # Now uses renamed columns (OpenContracts, etc.)
    df = add_expiration_date(df, contract_code, "TickerSymbol")
    df = _process_dataframe(df, contract_code)

    return df.sort("ExpirationDate")


def fetch_price_report(
    date: DateLike, contract_code: str, source_type: Literal["PR", "SPR"] = "SPR"
) -> pl.DataFrame:
    """Fetch and process B3 price report from website.

    Downloads a nested ZIP file containing XML price data from B3's official website,
    extracts the XML, parses it for the specified contract, and returns a standardized
    Polars DataFrame with contract details, pricing data, and calculated metrics.

    The function automatically determines column naming (Rate vs Price) based on the
    contract type:
    - Rate contracts (DI1, DAP, DDI, FRC, FRO): Columns like "OpenRate", "CloseRate"
    - Price contracts (DOL, WDO, IND, WIN, etc.): Columns like "OpenPrice", "ClosePrice"

    Additional calculated columns include:
    - BDaysToExp: Business days until expiration
    - DaysToExp: Calendar days until expiration
    - DV01: Dollar value of 1 basis point (for DI1 contracts)
    - ForwardRate: Forward rate calculated from settlement rate (for DI1, DAP)

    Args:
        date (DateLike): Trade date in format 'DD-MM-YYYY', 'DD/MM/YYYY', 'YYYY-MM-DD',
            or datetime.date object.
        contract_code (str): B3 contract code (e.g., 'DI1', 'DOL', 'DAP', 'FRC', 'DDI',
            'WDO', 'IND', 'WIN'). First 3 characters are used to match tickers in XML.
        source_type (str, optional): Type of price report file. 'SPR' for settlement
            price report (default), 'PR' for regular price report. Defaults to "SPR".

    Returns:
        pl.DataFrame: DataFrame with columns ordered as per OUTPUT_COLUMNS, filtered to
            exclude expired contracts (DaysToExp <= 0). Contains:
            - Identification: TradeDate, TickerSymbol, ExpirationDate
            - Volume & Count: TradeCount, TradeVolume, FinancialVolume, OpenContracts
            - Pricing: Settlement data, Open/High/Low/Close prices or rates
            - Metrics: DaysToExp, BDaysToExp, DV01, ForwardRate, etc.
            Empty DataFrame if no data available or date is invalid.

    Raises:
        ValueError: If source_type is invalid (not 'PR' or 'SPR').
        DataNotAvailableError: If the date is valid but has no price report data.
        requests.HTTPError: If the download request fails with HTTP error.

    Example:
        >>> import pyield as yd
        >>> df = yd.fetch_price_report("26-04-2024", "DI1")
        >>> df.columns[:5]
        ['TradeDate', 'TickerSymbol', 'ExpirationDate', 'BDaysToExp', 'DaysToExp']
        >>> df.shape[0] > 0  # Check if we have contracts
        True

        >>> # Handle a weekend or holiday (returns empty DataFrame)
        >>> df = yd.fetch_price_report("25-12-2023", "DI1")  # Christmas Eve
        >>> df.is_empty()
        True
    """
    empty_msg = f"No data for {contract_code} on {date}. Returning empty DataFrame."
    if has_nullable_args(date):
        logger.warning(empty_msg)
        return pl.DataFrame()

    try:
        date = cv.convert_dates(date)
        zip_data = _fetch_zip_from_url(date, source_type)

        if not zip_data:
            logger.warning(empty_msg)
            return pl.DataFrame()

        df = process_zip_file(zip_data, contract_code, source_type)

        if df.is_empty():
            logger.warning(empty_msg)

        return df

    except (ValueError, DataNotAvailableError, requests.HTTPError):
        raise
    except (zipfile.BadZipFile, etree.XMLSyntaxError):
        logger.warning(f"Failed to parse price report for {contract_code} on {date}")
        return pl.DataFrame()
    except Exception:
        logger.exception(
            f"CRITICAL: Failed to process {contract_code} {source_type} for {date}"
        )
        return pl.DataFrame()


def read_price_report(
    file_path: Path,
    contract_code: str,
    source_type: Literal["PR", "SPR"] | None = None,
) -> pl.DataFrame:
    """Read and process B3 price report from a local ZIP file."""
    if source_type is None:
        filename = file_path.name
        source_type = "SPR" if filename.startswith("SPRD") else "PR"

    zip_data = _fetch_zip_from_file(file_path)
    df = process_zip_file(zip_data, contract_code, source_type)
    return df
