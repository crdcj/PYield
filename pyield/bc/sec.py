import logging

import pandas as pd

from pyield.date_converter import DateScalar, convert_input_dates
from pyield.retry import default_retry

logger = logging.getLogger(__name__)

COLUMN_MAPPING = {
    "DATA MOV": "SettlementDate",
    "SIGLA": "BondType",
    "CODIGO": "SelicCode",
    "CODIGO ISIN": "ISIN",
    "EMISSAO": "IssueDate",
    "VENCIMENTO": "Maturity",
    "NUM DE OPER": "TradeCount",
    "QUANT NEGOCIADA": "TradeQuantity",
    "VALOR NEGOCIADO": "TradeValue",
    "PU MIN": "MinPrice",
    "PU MED": "AvgPrice",
    "PU MAX": "MaxPrice",
    "PU LASTRO": "UnderlyingPrice",
    "VALOR PAR": "ParValue",
    "TAXA MIN": "MinRate",
    "TAXA MED": "AvgRate",
    "TAXA MAX": "MaxRate",
    "NUM OPER COM CORRETAGEM": "BrokerageTradeCount",
    "QUANT NEG COM CORRETAGEM": "BrokerageTradeQuantity",
}


def _build_download_url(target_date: DateScalar) -> str:
    """
    URL com todos os arquivos disponíveis:
    https://www4.bcb.gov.br/pom/demab/negociacoes/apresentacao.asp?frame=1

    Exemplo de URL para download:
    https://www4.bcb.gov.br/pom/demab/negociacoes/download/NegE202409.ZIP

    File format: NegEYYYYMM.ZIP
    """
    target_date = convert_input_dates(target_date)
    file_date = target_date.strftime("%Y%m")
    file_name = f"NegE{file_date}.ZIP"
    base_url = "https://www4.bcb.gov.br/pom/demab/negociacoes/download"
    return f"{base_url}/{file_name}"


@default_retry
def _fetch_data_from_url(file_url: str) -> pd.DataFrame:
    df = pd.read_csv(
        file_url,
        sep=";",
        decimal=",",
        dtype_backend="numpy_nullable",
    )
    for col in ["DATA MOV", "EMISSAO", "VENCIMENTO"]:
        df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")

    return df


def sec(target_date: DateScalar) -> pd.DataFrame:
    """Fetches bond trading data for a specific date from the Brazilian Central Bank.

    Downloads the daily bond trading data from the Brazilian Central Bank
    website for the given target date.  The data is downloaded as a ZIP file,
    extracted, and read into a Pandas DataFrame.  The columns are then
    renamed to English names for easier use.

    Args:
        target_date (DateScalar): The date for which to fetch the bond
            trading data.  This can be a date-like object understood by
            `pyield.date_converter.convert_input_dates`.

    Returns:
        pd.DataFrame: A DataFrame containing the bond trading data for the
            specified date.  Columns are renamed according to `COLUMN_MAPPING`
            for English readability.
    """
    url = _build_download_url(target_date)
    df = _fetch_data_from_url(url)
    df = df.rename(columns=COLUMN_MAPPING)
    return df
