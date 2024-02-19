import io
import zipfile
from pathlib import Path

import requests
import pandas as pd
from lxml import etree

from . import di_futures as dif
from . import br_calendar as brc


def get_file_from_url(reference_date) -> io.BytesIO:
    reference_date = pd.Timestamp(reference_date)
    reference_date_str = reference_date.strftime("%y%m%d")
    # url example: https://www.b3.com.br/pesquisapregao/download?filelist=PR231228.zip
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{reference_date_str}.zip"

    response = requests.get(url)
    # If size is less than 1 KB, then the file does not exist
    if response.status_code != 200 or len(response.content) < 1024:
        raise ValueError(
            f"Não existe dado disponível para a data {reference_date.strftime('%Y-%m-%d')}."
        )
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


def extract_di1_data_from_xml(xml_file: io.BytesIO) -> list:
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
    di1_data = []

    # Processar cada TckrSymb encontrado
    for tckr_symb in tckr_symbols:
        # Subir na hierarquia para encontrar o PricRpt pai
        pric_rpt = tckr_symb.getparent().getparent()

        # Encontrar o elemento FinInstrmAttrbts dentro de PricRpt
        fin_instrm_attrbts = pric_rpt.find(".//ns:FinInstrmAttrbts", namespaces)

        # Verificar se FinInstrmAttrbts existe
        if fin_instrm_attrbts is None:
            continue  # Pular para o próximo TckrSymb se FinInstrmAttrbts não existir
        # Dicionário para armazenar os dados de um TckrSymb
        tckr_data = {"TckrSymb": tckr_symb.text}

        # Iterar sobre cada filho de FinInstrmAttrbts
        for attr in fin_instrm_attrbts:
            tag_name = etree.QName(attr).localname
            tckr_data[tag_name] = attr.text

        # Adicionar o dicionário à lista
        di1_data.append(tckr_data)

    return di1_data


def create_df_from_di1_data(di1_data: list) -> pd.DataFrame:
    # Criar um DataFrame com os dados coletados
    df = pd.DataFrame(di1_data)

    # Convert to CSV and then back to pandas to get automatic type conversion
    file = io.StringIO(df.to_csv(index=False))
    return pd.read_csv(file, dtype_backend="numpy_nullable")


def process_df(df_raw: pd.DataFrame, reference_date: pd.Timestamp) -> pd.DataFrame:
    df = df_raw.copy()
    # Remover colunas cujos dados são constantes: MktDataStrmId, AdjstdQtStin e PrvsAdjstdQtStin
    df.drop(columns=["MktDataStrmId", "AdjstdQtStin", "PrvsAdjstdQtStin"], inplace=True)

    df.insert(0, "RptDt", reference_date)
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["RptDt"] = df["RptDt"].astype("datetime64[ns]")

    expiration = df["TckrSymb"].str[3:].apply(dif.get_expiration_date)
    # Insert the column at the beginning
    df.insert(2, "ExpDt", expiration)

    business_days = brc.count_bdays(reference_date, df["ExpDt"])
    # Insert the column at the beginning
    df.insert(3, "BDaysToExp", business_days)
    # Convert to nullable integer, since other columns use this data type
    df["BDaysToExp"] = df["BDaysToExp"].astype(pd.Int64Dtype())
    # Remove expired contracts
    df.query("BDaysToExp > 0", inplace=True)

    return df.sort_values(by=["ExpDt"], ignore_index=True)


def get_di(
    reference_date: pd.Timestamp, data_path: Path, return_raw: bool
) -> pd.DataFrame:
    if data_path:
        # Filename example: PR231228.zip
        reference_date_str = reference_date.strftime("%y%m%d")
        filepath = data_path / f"PR{reference_date_str}.zip"
        if not filepath.exists():
            raise FileNotFoundError(
                f"O arquivo PR{reference_date_str}.zip não foi encontrado em {data_path}"
            )
        else:
            content = filepath.read_bytes()
            zip_file = io.BytesIO(content)
    else:
        # Read the file from the internet
        zip_file = get_file_from_url(reference_date)

    xml_file = extract_xml_from_zip(zip_file)
    xml_data = extract_di1_data_from_xml(xml_file)

    raw_df = create_df_from_di1_data(xml_data)
    if return_raw:
        return raw_df
    return process_df(raw_df, reference_date)
