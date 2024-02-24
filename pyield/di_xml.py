import io
import zipfile
from pathlib import Path

import requests
import pandas as pd
from lxml import etree

from . import di_futures as dif
from . import br_calendar as brc


def get_file_from_url(trade_date: pd.Timestamp, source_type: str) -> io.BytesIO:
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


def extract_xml_from_zip(zip_file: io.BytesIO) -> io.BytesIO:
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


def extract_di_data_from_xml(xml_file: io.BytesIO) -> list:
    parser = etree.XMLParser(
        ns_clean=True, remove_blank_text=True, remove_comments=True, recover=True
    )
    tree = etree.parse(xml_file, parser)
    namespaces = {"ns": "urn:bvmf.217.01.xsd"}

    # XPath para encontrar elementos cujo texto começa com "DI1"
    tckr_symbols = tree.xpath(
        '//ns:TckrSymb[starts-with(text(), "DI1")]', namespaces=namespaces
    )

    # Lista para armazenar os dados
    di_data = []

    # Processar cada TckrSymb encontrado
    for tckr_symb in tckr_symbols:
        # Acessar o elemento PricRpt que contém o TckrSymb
        price_report = tckr_symb.getparent().getparent()

        # Extrair a data de negociação
        trade_date = price_report.find(".//ns:TradDt/ns:Dt", namespaces)

        # Preparar o dicionário de dados do ticker com a data de negociação
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


def create_df_from_di_data(di1_data: list) -> pd.DataFrame:
    # Criar um DataFrame com os dados coletados
    df = pd.DataFrame(di1_data)

    # Convert to CSV and then back to pandas to get automatic type conversion
    file = io.StringIO(df.to_csv(index=False))
    return pd.read_csv(file, dtype_backend="numpy_nullable")


def filter_pr_df(df: pd.DataFrame) -> pd.DataFrame:
    selected_columns = [
        "TradDt",
        "TckrSymb",
        # "MktDataStrmId",
        "NtlFinVol",
        # "IntlFinVol",
        "OpnIntrst",
        "FinInstrmQty",
        "BestBidPric",
        "BestAskPric",
        "FrstPric",
        "MinPric",
        "MaxPric",
        "TradAvrgPric",
        "LastPric",
        "RglrTxsQty",
        "RglrTraddCtrcts",
        "NtlRglrVol",
        # "IntlRglrVol",
        "AdjstdQt",
        "AdjstdQtTax",
        # "AdjstdQtStin",
        # "PrvsAdjstdQt",
        # "PrvsAdjstdQtTax",
        # "PrvsAdjstdQtStin",
        # "OscnPctg",
        # "VartnPts",
        # "AdjstdValCtrct",
        "MaxTradLmt",
        "MinTradLmt",
    ]

    return df[selected_columns]


def filter_sprd_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "TradDt",
        "TckrSymb",
        "OpnIntrst",
        "FrstPric",
        "MinPric",
        "MaxPric",
        "TradAvrgPric",
        "LastPric",
        "RglrTxsQty",
        "AdjstdQt",
        "AdjstdQtTax",
        # "AdjstdQtStin",  # Constant column
        # "PrvsAdjstdQt",
        # "PrvsAdjstdQtTax",
        # "PrvsAdjstdQtStin",  # Constant column
    ]

    return df[cols]


def process_di_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["TradDt"] = df["TradDt"].astype("datetime64[ns]")

    expiration = df["TckrSymb"].str[3:].apply(dif.get_expiration_date)
    df.insert(2, "ExpDt", expiration)

    business_days = brc.count_bdays(df["TradDt"], df["ExpDt"])
    df.insert(3, "BDToExp", business_days)

    # Convert to nullable integer, since other columns use this data type
    df["BDToExp"] = df["BDToExp"].astype(pd.Int64Dtype())

    # Remove expired contracts
    df.query("BDToExp > 0", inplace=True)

    return df.sort_values(by=["ExpDt"], ignore_index=True)


def get_di(
    trade_date: pd.Timestamp,
    source_type: str,
    return_raw: bool,
) -> pd.DataFrame:
    zip_file = get_file_from_url(trade_date, source_type)

    xml_file = extract_xml_from_zip(zip_file)

    di_data = extract_di_data_from_xml(xml_file)

    raw_df = create_df_from_di_data(di_data)
    if return_raw:
        return raw_df

    # Remove unnecessary columns
    if source_type == "b3":
        di_df = filter_pr_df(raw_df)
    elif source_type == "b3s":
        di_df = filter_sprd_df(raw_df)

    return process_di_df(di_df)


def read_di(file_path: Path, return_raw: bool = False) -> pd.DataFrame:
    if file_path:
        if file_path.exists():
            content = file_path.read_bytes()
            zip_file = io.BytesIO(content)
        else:
            raise FileNotFoundError(f"No file found at {file_path}.")

        xml_file = extract_xml_from_zip(zip_file)

        di_data = extract_di_data_from_xml(xml_file)

        raw_df = create_df_from_di_data(di_data)
        if return_raw:
            return raw_df

        # Filename examples: PR231228.zip or SPRD240216.zip
        file_stem = file_path.stem
        if "PR" in file_stem:
            df_di = filter_pr_df(raw_df)
        elif "SPRD" in file_stem:
            df_di = filter_sprd_df(raw_df)
        else:
            raise ValueError("Filename must start with 'PR' or 'SPRD'.")

        return process_di_df(df_di)

    else:
        raise ValueError("A file path must be provided.")
