import io
import logging
import zipfile
from pathlib import Path
from typing import Literal

import pandas as pd
import requests
from lxml import etree

from pyield import bday
from pyield.b3.futures import common
from pyield.fwd import forwards
from pyield.retry import default_retry

logger = logging.getLogger(__name__)


def _get_file_from_path(file_path: Path) -> io.BytesIO:
    # Check if a file path was not provided
    if not isinstance(file_path, Path):
        raise ValueError("A file path must be provided.")
    if not file_path.exists():
        raise FileNotFoundError(f"No file found at {file_path}.")

    content = file_path.read_bytes()
    return io.BytesIO(content)


@default_retry
def _get_file_from_url(date: pd.Timestamp, source_type: str) -> io.BytesIO:
    """
    Types of XML files available:
    Full Price Report (all assets)
        - aprox. 5 MB zipped file;
        - url example: https://www.b3.com.br/pesquisapregao/download?filelist=PR231228.zip
    Simplified Price Report (derivatives)
        - aprox. 50kB zipped file;
        url example: https://www.b3.com.br/pesquisapregao/download?filelist=SPRD240216.zip
    """

    date_str = date.strftime("%y%m%d")

    if source_type == "PR":
        url = f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{date_str}.zip"
    elif source_type == "SPR":
        url = (
            f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRD{date_str}.zip"
        )
    else:
        raise ValueError("Invalid source type. Must be either 'PR' or 'SPR'.")

    response = requests.get(url, timeout=10)

    # When a the date has no data, the file has less than 22 bytes
    if len(response.content) < 1024:  # noqa
        date_str = date.strftime("%Y-%m-%d")
        return io.BytesIO()

    return io.BytesIO(response.content)


def _extract_xml_from_zip(zip_file: io.BytesIO) -> io.BytesIO:
    # First, read the outer file
    with zipfile.ZipFile(zip_file, "r") as outer_zip:
        outer_file_name = outer_zip.namelist()[0]
        outer_file_content = outer_zip.read(outer_file_name)
    outer_file = io.BytesIO(outer_file_content)

    # Then, read the inner file
    with zipfile.ZipFile(outer_file, "r") as inner_zip:
        filenames = inner_zip.namelist()
        # Filter only xml files
        xml_filenames = [name for name in filenames if name.endswith(".xml")]
        xml_filenames.sort()
        # Unzip last file (the most recent as per B3's name convention)
        inner_file_content = inner_zip.read(xml_filenames[-1])

    return io.BytesIO(inner_file_content)


def _extract_data_from_xml(xml_file: io.BytesIO, asset_code: str) -> list[dict]:
    parser = etree.XMLParser(
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        recover=True,
        resolve_entities=False,
        no_network=True,
        load_dtd=False,  # Disable DTD loading
    )
    tree = etree.parse(xml_file, parser=parser)

    # XPath para encontrar elementos cujo texto começa com código do ativo: DI1, FRC...
    namespaces = {"ns": "urn:bvmf.217.01.xsd"}
    path = f'//ns:TckrSymb[starts-with(text(), "{asset_code}")]'
    tickers = tree.xpath(path, namespaces=namespaces)

    if tickers is None or not isinstance(tickers, list) or len(tickers) == 0:
        return []

    # Lista para armazenar os dados
    di_data = []

    # Processar cada TckrSymb encontrado
    for ticker in tickers:
        if not isinstance(ticker, etree._Element):
            continue

        # A future contract ticker must have 6 characters
        if ticker.text is None or len(ticker.text) != 6:  # noqa
            continue

        # Extract the price report element
        parent = ticker.getparent()
        if parent is None:
            continue
        price_report = parent.getparent()
        if price_report is None:
            continue

        # Extract the trade date
        date = price_report.find(".//ns:TradDt/ns:Dt", namespaces)
        if date is None:
            continue

        # Store the data in a dictionary
        ticker_data = {"TradDt": date.text, "TckrSymb": ticker.text}

        # Extract the FinInstrmAttrbts element
        fin_instrm_attrbts = price_report.find(".//ns:FinInstrmAttrbts", namespaces)
        # Verificar se FinInstrmAttrbts existe
        if fin_instrm_attrbts is None:
            continue  # Pular para o próximo TckrSymb se FinInstrmAttrbts não existir

        # Extrair os dados de FinInstrmAttrbts
        for attr in fin_instrm_attrbts:
            tag_name = etree.QName(attr).localname
            ticker_data[tag_name] = attr.text

        # Adicionar o dicionário à lista
        di_data.append(ticker_data)

    return di_data


def _create_df_from_data(di1_data: list) -> pd.DataFrame:
    # Criar um DataFrame com os dados coletados
    df = pd.DataFrame(di1_data)

    # Convert to CSV and then back to pandas to get automatic type conversion
    file = io.StringIO(df.to_csv(index=False))
    return pd.read_csv(file, dtype_backend="numpy_nullable")


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    all_columns = {
        "TradDt": "TradeDate",
        "TckrSymb": "TickerSymbol",
        # "MktDataStrmId"
        # "IntlFinVol",
        "OpnIntrst": "OpenContracts",
        "FinInstrmQty": "TradeVolume",
        "NtlFinVol": "FinancialVolume",
        "AdjstdQt": "SettlementPrice",
        "MinTradLmt": "MinLimitRate",
        "MaxTradLmt": "MaxLimitRate",
        # Must invert bid/ask for rates
        "BestAskPric": "BestBidRate",
        "BestBidPric": "BestAskRate",
        "MinPric": "MinRate",
        "TradAvrgPric": "AvgRate",
        "MaxPric": "MaxRate",
        "FrstPric": "OpenRate",
        "LastPric": "CloseRate",
        "AdjstdQtTax": "SettlementRate",
        "RglrTxsQty": "TradeCount",
        # "RglrTraddCtrcts"
        # "NtlRglrVol"
        # "IntlRglrVol",
        # "AdjstdQtStin",
        # "PrvsAdjstdQt",
        # "PrvsAdjstdQtTax",
        # "PrvsAdjstdQtStin",
        # "OscnPctg",
        # "VartnPts",
        # "AdjstdValCtrct",
    }
    all_columns = {c: all_columns[c] for c in all_columns if c in df.columns}
    return df.rename(columns=all_columns)


def _process_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["TradeDate"] = df["TradeDate"].astype("datetime64[ns]")

    expiration_code = df["TickerSymbol"].str[3:]
    contract_code = df["TickerSymbol"].str[:3].loc[0]
    expiration_day = 15 if contract_code == "DAP" else 1
    df["ExpirationDate"] = expiration_code.apply(
        common.get_expiration_date, args=(expiration_day,)
    )

    df["DaysToExp"] = (df["ExpirationDate"] - df["TradeDate"]).dt.days
    # Convert to nullable integer, since it is the default type in the library
    df["DaysToExp"] = df["DaysToExp"].astype("Int64")

    # Remove expired contracts
    df = df.query("DaysToExp > 0").reset_index(drop=True)

    df["BDaysToExp"] = bday.count(df["TradeDate"], df["ExpirationDate"])

    rate_cols = [col for col in df.columns if "Rate" in col]
    # Remove % and round to 5 (3 in %) dec. places in rate columns
    df[rate_cols] = df[rate_cols].div(100).round(5)

    # Columns where NaN means 0
    zero_cols = ["OpenContracts", "TradeCount", "TradeVolume", "FinancialVolume"]
    for col in zero_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    if contract_code == "DI1":  # Calculate DV01 for DI1 contracts
        duration = df["BDaysToExp"] / 252
        modified_duration = duration / (1 + df["SettlementRate"])
        df["DV01"] = 0.0001 * modified_duration * df["SettlementPrice"]

    if contract_code in {"DI1", "DAP"}:  # Calculate forwards for DI1 and DAP contracts
        df["ForwardRate"] = forwards(bdays=df["BDaysToExp"], rates=df["SettlementRate"])

    return df.sort_values(by=["ExpirationDate"], ignore_index=True)


def _select_and_reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    # All SPRD columns are present in PR
    all_columns = [
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
        "MinLimitRate",
        "MaxLimitRate",
        "BestBidRate",
        "BestAskRate",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "CloseRate",
        "SettlementRate",
        "ForwardRate",
    ]
    selected_columns = [col for col in all_columns if col in df.columns]
    return df[selected_columns]


def process_zip_file(zip_file: io.BytesIO, contract_code: str) -> pd.DataFrame:
    if zip_file is None or zip_file.getbuffer().nbytes == 0:
        logger.warning("Empty XML zip file. Probably the date has no data.")
        return pd.DataFrame()

    xml_file = _extract_xml_from_zip(zip_file)

    di_data = _extract_data_from_xml(xml_file, contract_code)

    df_raw = _create_df_from_data(di_data)

    df = _rename_columns(df_raw)

    df = _process_df(df)

    df = _select_and_reorder_columns(df)

    return df


def fetch_xml_data(
    date: pd.Timestamp, contract_code: str, source_type: Literal["PR", "SPR"]
) -> pd.DataFrame:
    """Fetches and processes an XML report from B3's website.

    Downloads a zipped XML report for a specific date and asset code
    from B3's website, extracts the relevant data, and returns it as a
    Pandas DataFrame.

    Args:
        date: The date of the report to fetch.
        asset_code: The asset code to filter the report for (e.g., 'DI1').
        source_type: The type of report to fetch, either 'PR' (Full Price
            Report) or 'SPR' (Simplified Price Report).

    Returns:
        A Pandas DataFrame containing the processed data.
        For 'DI1' asset codes, an additional 'DV01' column is calculated.

    Raises:
        ValueError: If the `source_type` is invalid or if no data is
            available for the given date.
    """
    try:
        zip_file = _get_file_from_url(date, source_type)
        df = process_zip_file(zip_file, contract_code)
    except ValueError as e:
        logger.warning(f"Error fetching XML data: {e}. Returning empty DataFrame.")
        return pd.DataFrame()

    return df


def read_xml_report(file_path: Path, contract_code: str) -> pd.DataFrame:
    """Reads and processes an XML report from a local file.

    Reads a zipped XML report from the specified file path, extracts the
    relevant data, and returns it as a Pandas DataFrame.

    Args:
        file_path: The path to the zipped XML report file.
        asset_code: The asset code to filter the report for (e.g., 'DI1').

    Returns:
        A Pandas DataFrame containing the processed data.
        For 'DI1' asset codes, an additional 'DV01' column is calculated.

    Raises:
        ValueError: If the provided `file_path` is not a Path object.
        FileNotFoundError: If no file is found at the specified `file_path`.
    """
    zip_file = _get_file_from_path(file_path)
    df = process_zip_file(zip_file, contract_code)
    return df
