import io
import logging
import re
import warnings

import pandas as pd
import requests
from bs4 import XMLParsedAsHTMLWarning

from pyield import date_converter as dc
from pyield.anbima.tpf import tpf_data
from pyield.date_converter import DateScalar

# Configura o logger do módulo
logger = logging.getLogger(__name__)

# --- Configurações Centralizadas ---
IMA_URL = "https://www.anbima.com.br/informacoes/ima/ima-quantidade-mercado.asp"
COLUMN_MAPPING = {
    "Data Referência": "Date",
    "Título": "BondType",
    "Data de Vencimento": "MaturityDate",
    "Codigo Selic": "SelicCode",
    "Código ISIN": "ISIN",
    "PU (R$)": "Price",
    "Quantidade em Mercado (1.000 Títulos)": "MarketQuantity",
    "Valor de Mercado (R$ Mil)": "MarketValue",
    "Variação da Quantidade (1.000 Títulos)": "QuantityVariation",
    "Status do Titulo": "BondStatus",
}


def _fetch_url_content(target_date: pd.Timestamp) -> str:
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
        return ""
    return r.text


def _fetch_url_tables(text: str) -> pd.DataFrame:
    # Warning supression for BeautifulSoup, as the text is valid HTML and not XML
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

    if not text:
        return pd.DataFrame()

    dfs = pd.read_html(
        io.StringIO(text),
        flavor="html5lib",
        attrs={"width": "100%"},
        header=0,
        thousands=".",
        decimal=",",
        displayed_only=True,
        dtype_backend="pyarrow",
        na_values="--",
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
    df = pd.read_csv(csv_buffer, dtype_backend="pyarrow", engine="pyarrow")

    # Expressão regular para datas no formato dd/mm/ano
    date_pattern = r"\b(\d{2}/\d{2}/\d{4})\b"

    # Busca pela primeira data
    match = re.search(date_pattern, text)

    if match:
        anbima_date_str = match.group(1)
        anbima_date = pd.to_datetime(anbima_date_str, format="%d/%m/%Y")
    else:
        return pd.DataFrame()

    df["Data Referência"] = anbima_date

    return df


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_MAPPING)[COLUMN_MAPPING.values()]
    df["MaturityDate"] = pd.to_datetime(df["MaturityDate"], format="%d/%m/%Y")
    df["MaturityDate"] = df["MaturityDate"].astype("date32[pyarrow]")
    df["Date"] = df["Date"].astype("date32[pyarrow]")

    for col in ["MarketQuantity", "MarketValue", "QuantityVariation"]:
        # Fallback to string conversion in case conversion failed during read_csv
        if pd.api.types.is_string_dtype(df[col].dtype):
            df[col] = df[col].str.replace(".", "", regex=False).replace(",", ".")
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Remove the thousands unit from numeric columns
        df[col] = (1000 * df[col]).round(0).astype("int64[pyarrow]")

    return df.sort_values(by=["BondType", "MaturityDate"]).reset_index(drop=True)


def _add_dv01(df: pd.DataFrame) -> pd.DataFrame:
    target_date = df["Date"].min()
    df_anbima = tpf_data(target_date)
    target_cols = ["ReferenceDate", "BondType", "MaturityDate", "DV01", "DV01USD"]
    df_anbima = df_anbima[target_cols].rename(columns={"ReferenceDate": "Date"})
    # Guard clause for missing columns
    if "DV01" not in df_anbima.columns or "DV01USD" not in df_anbima.columns:
        return df

    df = df.merge(df_anbima, on=["Date", "BondType", "MaturityDate"], how="left")
    # Calcular os estoques
    df["MarketDV01"] = df["DV01"] * df["MarketQuantity"]
    df["MarketDV01USD"] = df["DV01USD"] * df["MarketQuantity"]
    for col in ["MarketDV01", "MarketDV01USD"]:
        df[col] = df[col].round(0).astype("int64[pyarrow]")
    # Remover colunas desnecessárias
    df = df.drop(columns=["DV01", "DV01USD"], errors="ignore")
    return df


def _reorder_df(df: pd.DataFrame) -> pd.DataFrame:
    # Reorder the DataFrame columns
    column_order = [
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
    return df[column_order].copy()


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
    date_str = date.strftime("%d/%m/%Y")
    try:
        text = _fetch_url_content(date)
        df = _fetch_url_tables(text)
        if df.empty:
            logger.warning(f"No data for {date_str}. Returning an empty DataFrame.")
            return df
        df = _process_df(df)
        df = _add_dv01(df)
        df = _reorder_df(df)
        return df
    except Exception:  # Erro inesperado
        msg = f"Error fetching IMA for {date_str}. Returning empty DataFrame."
        logger.exception(msg)
        return pd.DataFrame()
