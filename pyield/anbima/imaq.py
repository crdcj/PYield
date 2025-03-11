import io
import logging
import re
import warnings

import pandas as pd
import requests
from bs4 import XMLParsedAsHTMLWarning

from pyield import date_converter as dc
from pyield.date_converter import DateScalar

# Configura o logger do módulo
logger = logging.getLogger(__name__)


IMA_URL = "https://www.anbima.com.br/informacoes/ima/ima-quantidade-mercado.asp"
COLUMN_MAPPING = {
    "Data Referência": "Date",
    "Título": "BondType",
    "Data de Vencimento": "Maturity",
    "Codigo Selic": "SelicCode",
    "Código ISIN": "ISIN",
    "PU (R$)": "Price",
    "Quantidade em Mercado (1.000 Títulos)": "MarketQuantity",
    "Valor de Mercado (R$ Mil)": "MarketValue",
    "Variação da Quantidade (1.000 Títulos)": "QuantityVariation",
    "Status do Titulo": "BondStatus",
}


def _fetch_url_tables(target_date: pd.Timestamp) -> pd.DataFrame:
    # Warning supression for BeautifulSoup, as the text is valid HTML and not XML
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

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
    r.encoding = "iso-8859-1"
    if "Não há dados disponíveis" in r.text:
        logger.warning(f"No data for {target_date_str}. Returning empty DataFrame.")
        return pd.DataFrame()

    string_io_buffer = io.StringIO(r.text)

    dfs = pd.read_html(
        string_io_buffer,
        flavor="html5lib",
        attrs={"width": "100%"},
        header=0,
        thousands=".",
        decimal=",",
        displayed_only=True,
        dtype_backend="numpy_nullable",
    )

    df = (
        pd.concat(dfs)
        .query("`Data de Vencimento`.notnull()")
        .query("Título!='Título'")
        .drop_duplicates(subset="Código ISIN")
        .reset_index(drop=True)
    )

    # Convert to CSV and then back to pandas to get automatic type conversion
    csv_buffer = io.StringIO(df.to_csv(index=False))
    df = pd.read_csv(csv_buffer, dtype_backend="numpy_nullable")

    # Expressão regular para datas no formato dd/mm/ano
    date_pattern = r"\b(\d{2}/\d{2}/\d{4})\b"

    # Busca pela primeira data
    match = re.search(date_pattern, r.text)

    if match:
        anbima_date_str = match.group(1)
        anbima_date = pd.to_datetime(anbima_date_str, format="%d/%m/%Y")
    else:
        logger.warning(f"No data for {target_date_str}. Returning empty DataFrame.")
        return pd.DataFrame()

    df["Data Referência"] = anbima_date

    return df


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_MAPPING)[COLUMN_MAPPING.values()]
    df["Maturity"] = pd.to_datetime(df["Maturity"], format="%d/%m/%Y")
    # Remove the thousands unit from numeric columns
    for col in ["MarketQuantity", "MarketValue", "QuantityVariation"]:
        df[col] = (1000 * df[col]).astype("Int64")

    return df


def imaq(date: DateScalar) -> pd.DataFrame:
    """
    Fetch and process IMA market data for a given date.

    This function retrieves IMA quantity market data from ANBIMA for a given date,
    processes the data into a structured DataFrame, and returns the resulting DataFrame.
    It handles conversion of date formats, renames columns to English, and converts
    certain numeric columns to integer types. In the event of an error during data
    fetching or processing, an empty DataFrame is returned.

    Args:
        date (DateScalar): A date-like object representing the target date for fetching
            the data.

    Returns:
        pd.DataFrame: A DataFrame containing the IMA data.

    DataFrame columns:
        - Date: Reference date of the data.
        - BondType: Type of bond.
        - Maturity: Bond maturity date.
        - SelicCode: Code representing the SELIC rate.
        - ISIN: International Securities Identification Number.
        - Price: Bond price.
        - MarketQuantity: Market quantity (in units of 1000 bonds).
        - MarketValue: Market value (in units of 1000 reais).
        - QuantityVariation: Variation in quantity (in units of 1000 bonds).
        - BondStatus: Status of the bond.

    Raises:
        Exception: Logs error and returns an empty DataFrame if any error occurs during
            fetching or processing.

    """
    date = dc.convert_input_dates(date)
    try:
        df = _fetch_url_tables(date)
        if df.empty:
            return df
        return _process_df(df)
    except Exception:  # Erro inesperado
        date_str = date.strftime("%d/%m/%Y")
        msg = f"Error fetching IMA for {date_str}. Returning empty DataFrame."
        logger.exception(msg)
        return pd.DataFrame()
