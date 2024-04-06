import io
import zipfile
from pathlib import Path

import requests
import pandas as pd
from pandas import Timestamp, DataFrame
from lxml import etree

from . import core as cr
from .. import calendar as cd


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

    if source_type == "b3":
        url = f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{formatted_date}.zip"
    else:  # source_type == "b3s"
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


def _filter_and_order_pr_df(df: DataFrame) -> DataFrame:
    selected_columns = [
        "TradDt",
        "TckrSymb",
        # "MktDataStrmId",
        "OpnIntrst",
        "FinInstrmQty",
        "NtlFinVol",
        # "IntlFinVol",
        "AdjstdQt",
        "MinTradLmt",
        "MaxTradLmt",
        "BestBidPric",
        "BestAskPric",
        "MinPric",
        "TradAvrgPric",
        "MaxPric",
        "FrstPric",
        "LastPric",
        "AdjstdQtTax",
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

    return df[selected_columns]


def _filter_and_order_sprd_df(df: DataFrame) -> DataFrame:
    cols = [
        "TradDt",
        "TckrSymb",
        "OpnIntrst",
        "AdjstdQt",
        "MinPric",
        "TradAvrgPric",
        "MaxPric",
        "FrstPric",
        "LastPric",
        "AdjstdQtTax",
        # "RglrTxsQty",
        # "AdjstdQtStin",  # Constant column
        # "PrvsAdjstdQt",
        # "PrvsAdjstdQtTax",
        # "PrvsAdjstdQtStin",  # Constant column
    ]

    return df[cols]


def _standardize_column_names(df: DataFrame) -> DataFrame:
    rename_dict = {
        "TradDt": "TradeDate",
        "TckrSymb": "Ticker",
        # "MktDataStrmId"
        # "IntlFinVol",
        "OpnIntrst": "OpenContracts",
        "FinInstrmQty": "TradedQuantity",
        "NtlFinVol": "FinancialVolume",
        "AdjstdQt": "SettlementPrice",
        "MinTradLmt": "MinTradeLimitRate",
        "MaxTradLmt": "MaxTradeLimitRate",
        # Must invert bid/ask for rates
        "BestAskPric": "BestBidRate",
        "BestBidPric": "BestAskRate",
        "MinPric": "MinRate",
        "TradAvrgPric": "AvgRate",
        "MaxPric": "MaxRate",
        "FrstPric": "FirstRate",
        "LastPric": "LastRate",
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

    return df.rename(columns=rename_dict)


def _process_di_df(df_raw: DataFrame) -> DataFrame:
    df = df_raw.copy()
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["TradDt"] = df["TradDt"].astype("datetime64[ns]")

    expiration = df["TckrSymb"].str[3:].apply(cr.get_expiration_date)
    df.insert(2, "ExpirationDate", expiration)

    business_days = cd.count_bdays(df["TradDt"], df["ExpirationDate"])
    df.insert(3, "BDToExpiration", business_days)

    # Convert to nullable integer, since other columns use this data type
    df["BDToExpiration"] = df["BDToExpiration"].astype(pd.Int64Dtype())

    # Remove expired contracts
    df.query("BDToExpiration > 0", inplace=True)

    return df.sort_values(by=["ExpirationDate"], ignore_index=True)


def get_di(trade_date: Timestamp, source_type: str, return_raw: bool) -> DataFrame:
    zip_file = _get_file_from_url(trade_date, source_type)

    xml_file = _extract_xml_from_zip(zip_file)

    di_data = _extract_di_data_from_xml(xml_file)

    df_raw = _create_df_from_di_data(di_data)
    if return_raw:
        return df_raw

    # Remove unnecessary columns
    if source_type == "b3":
        df_di = _filter_and_order_pr_df(df_raw)
    elif source_type == "b3s":
        df_di = _filter_and_order_sprd_df(df_raw)
    else:
        raise ValueError("Invalid source type. Must be 'b3' or 'b3s'.")

    # Process and transform data
    df_di = _process_di_df(df_di)

    # Standardize column names
    df_di = _standardize_column_names(df_di)

    return df_di


def read_di(file_path: Path, return_raw: bool = False) -> DataFrame:
    if file_path:
        if file_path.exists():
            content = file_path.read_bytes()
            zip_file = io.BytesIO(content)
        else:
            raise FileNotFoundError(f"No file found at {file_path}.")

        xml_file = _extract_xml_from_zip(zip_file)

        di_data = _extract_di_data_from_xml(xml_file)

        df_raw = _create_df_from_di_data(di_data)
        if return_raw:
            return df_raw

        # Filename examples: PR231228.zip or SPRD240216.zip
        file_stem = file_path.stem
        if "PR" in file_stem:
            df_di = _filter_and_order_pr_df(df_raw)
        elif "SPRD" in file_stem:
            df_di = _filter_and_order_sprd_df(df_raw)
        else:
            raise ValueError("Filename must start with 'PR' or 'SPRD'.")

        # Process and transform data
        df_di = _process_di_df(df_di)

        # Standardize column names
        df_di = _standardize_column_names(df_di)

        return df_di

    else:
        raise ValueError("A file path must be provided.")
