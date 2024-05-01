import io
import zipfile
from pathlib import Path

import pandas as pd
import requests
from lxml import etree
from pandas import DataFrame, Timestamp

import pyield as yd
from pyield.futures import historical as fh


def _get_file_from_url(trade_date: Timestamp, source_type: str) -> io.BytesIO:
    """
    Types of XML files available:
    Full Price Report (all assets)
        - aprox. 5 MB zipped file;
        - url example: https://www.b3.com.br/pesquisapregao/download?filelist=PR231228.zip
    Simplified Price Report (derivatives)
        - aprox. 50kB zipped file;
        url example: https://www.b3.com.br/pesquisapregao/download?filelist=SPRD240216.zip
    """

    formatted_date = trade_date.strftime("%y%m%d")

    if source_type == "PR":
        url = f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{formatted_date}.zip"
    else:  # source_type == "SPR"
        url = f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRD{formatted_date}.zip"

    response = requests.get(url)

    # File will be considered invalid if it is too small
    if response.status_code != 200 or len(response.content) < 1024:
        formatted_date = trade_date.strftime("%Y-%m-%d")
        raise ValueError(f"There is no data available for {formatted_date}.")

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


def _extract_di_data_from_xml(xml_file: io.BytesIO) -> list[dict]:
    parser = etree.XMLParser(
        ns_clean=True, remove_blank_text=True, remove_comments=True, recover=True
    )
    tree = etree.parse(xml_file, parser)
    namespaces = {"ns": "urn:bvmf.217.01.xsd"}

    # XPath para encontrar elementos cujo texto começa com "DI1"
    tckr_symbols = tree.xpath(
        '//ns:TckrSymb[starts-with(text(), "DI1")]', namespaces=namespaces
    )

    if (
        tckr_symbols is None
        or not isinstance(tckr_symbols, list)
        or len(tckr_symbols) == 0
    ):
        return []

    # Lista para armazenar os dados com type hinting
    # di_data: list[dict] = []
    di_data = []

    # Processar cada TckrSymb encontrado
    for tckr_symb in tckr_symbols:
        if isinstance(tckr_symb, etree._Element):
            price_report = tckr_symb.getparent()
            if price_report is not None:
                price_report = price_report.getparent()
            else:
                # Handle the case where tckr_symb doesn't have a parent
                continue
        else:
            # Handle the case where tckr_symb is not an _Element
            continue

        # Extrair a data de negociação
        if price_report is None:
            continue
        trade_date = price_report.find(".//ns:TradDt/ns:Dt", namespaces)

        # Preparar o dicionário de dados do ticker com a data de negociação
        if trade_date is None:
            continue
        ticker_data = {"TradDt": trade_date.text, "TckrSymb": tckr_symb.text}

        # Acessar o elemento FinInstrmAttrbts que contém o TckrSymb
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


def _create_df_from_di_data(di1_data: list) -> DataFrame:
    # Criar um DataFrame com os dados coletados
    df = pd.DataFrame(di1_data)

    # Convert to CSV and then back to pandas to get automatic type conversion
    file = io.StringIO(df.to_csv(index=False))
    return pd.read_csv(file, dtype_backend="numpy_nullable")


def _rename_columns(df: DataFrame) -> DataFrame:
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
        # "RglrTxsQty"
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


def _select_and_reorder_columns(df: DataFrame) -> DataFrame:
    # All SPRD columns are present in PR
    all_columns = [
        "TradeDate",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        # "MktDataStrmId",
        "OpenContracts",
        "TradeVolume",
        "FinancialVolume",
        # "IntlFinVol",
        "SettlementPrice",
        "SettlementRate",
        "MinLimitRate",
        "MaxLimitRate",
        "BestBidRate",
        "BestAskRate",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "CloseRate",
        # "RglrTxsQty",
        # "RglrTraddCtrcts",
        # "NtlRglrVol",
        # "IntlRglrVol",
        # "AdjstdQtStin",
        # "PrvsAdjstdQt",
        # "PrvsAdjstdQtTax",
        # "PrvsAdjstdQtStin",
        # "OscnPctg",
        # "VartnPts",
        # "AdjstdValCtrct",
    ]
    selected_columns = [col for col in all_columns if col in df.columns]
    return df[selected_columns]


def _process_df(df_raw: DataFrame) -> DataFrame:
    df = df_raw.copy()
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["TradeDate"] = df["TradeDate"].astype("datetime64[ns]")

    expiration_code = df["TickerSymbol"].str[3:]
    df["ExpirationDate"] = expiration_code.apply(fh.get_expiration_date)

    df["DaysToExp"] = (df["ExpirationDate"] - df["TradeDate"]).dt.days
    # Convert to nullable integer, since it is the default type in the library
    df["DaysToExp"] = df["DaysToExp"].astype(pd.Int64Dtype())
    # Remove expired contracts
    df.query("DaysToExp > 0", inplace=True)

    df["BDaysToExp"] = yd.bday.count_bdays(df["TradeDate"], df["ExpirationDate"])

    rate_cols = [col for col in df.columns if "Rate" in col]
    # Remove % and round to 5 (3 in %) dec. places in rate columns
    df[rate_cols] = df[rate_cols].div(100).round(5)

    # Columns where NaN means 0
    zero_cols = ["OpenContracts", "TradeVolume", "FinancialVolume"]
    for col in zero_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    return df.sort_values(by=["ExpirationDate"], ignore_index=True)


def fetch_di(trade_date: Timestamp, source_type: str) -> DataFrame:
    zip_file = _get_file_from_url(trade_date, source_type)

    xml_file = _extract_xml_from_zip(zip_file)

    di_data = _extract_di_data_from_xml(xml_file)

    df_raw = _create_df_from_di_data(di_data)

    df = _rename_columns(df_raw)

    df = _process_df(df)

    df = _select_and_reorder_columns(df)

    return df


def read_di(file_path: Path) -> DataFrame:
    content = file_path.read_bytes()
    zip_file = io.BytesIO(content)

    xml_file = _extract_xml_from_zip(zip_file)

    di_data = _extract_di_data_from_xml(xml_file)

    df_raw = _create_df_from_di_data(di_data)

    df = _rename_columns(df_raw)

    df = _process_df(df)

    df = _select_and_reorder_columns(df)

    return df


def read_file(file_path: Path, return_raw: bool = False) -> pd.DataFrame:
    """
    Reads DI futures data from a file and returns it as a pandas DataFrame.

    This function opens and reads a DI futures data file, returning the contents as a
    pandas DataFrame. It supports reading from both XML files provided by B3, wich
    are the simplified and complete Price Reports.

    Args:
        file_path (Path): The file path to the DI data file. This should be a valid
            Path object pointing to the location of the file.
        return_raw (bool, optional): If set to True, the function returns the raw data
            without applying any transformation or processing. Useful for cases where
            raw data inspection or custom processing is needed. Defaults to False.
        source_type (Literal["bmf", "PR", "SPR"], optional): Indicates the source of
            the data. Defaults to "bmf". Options include:
                - "bmf": Fetches data from the old BM&FBOVESPA website. Fastest option.
                - "PR": Fetches data from the complete Price Report (XML file) provided
                    by B3.
                - "SPR": Fetches data from the simplified Price Report (XML file)
                    provided by B3. Faster than "PR" but less detailed.

    Returns:
        pd.DataFrame: A DataFrame containing the processed or raw DI futures data,
            depending on the `return_raw` flag.

    Examples:
        >>> read_di(Path("path/to/di_data_file.xml"))
        # returns a DataFrame with the DI futures data

        >>> read_di(Path("path/to/di_data_file.xml"), return_raw=True)
        # returns a DataFrame with the raw DI futures data, without processing

    Note:
        The ability to process and return raw data is primarily intended for advanced
        users who require access to the data in its original form for custom analyses.
    """
    # Check if a file path was not provided
    if not isinstance(file_path, Path):
        raise ValueError("A file path must be provided.")
    if not file_path.exists():
        raise FileNotFoundError(f"No file found at {file_path}.")
    return read_di(file_path, return_raw=return_raw)
