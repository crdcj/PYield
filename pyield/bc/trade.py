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


def tpf_trades(target_date: DateScalar) -> pd.DataFrame:
    """Fetches monthly secondary trading data for the 'títulos públicos federais' (TPF)
    registered in the Brazilian Central Bank (BCB) system.

    Downloads the monthly bond trading data from the Brazilian Central Bank (BCB)
    website for the month corresponding to the provided date. The data is downloaded as
    a ZIP file, extracted, and loaded into a Pandas DataFrame. The data contains all
    trades executed during the month, separated by each 'SettlementDate'.

    Args:
        target_date (DateScalar): The date for which the monthly trading data will be
            fetched. This date can be a string, datetime, or pandas Timestamp object.
            It will be converted to a pandas Timestamp object. Only the year and month
            of this date will be used to download the corresponding monthly file.

    Returns:
        pd.DataFrame: A DataFrame containing the bond trading data for the
            specified month.

    DataFrame columns:
        - SettlementDate: Date when the trade settled
        - BondType: Security type abbreviation
        - SelicCode: Unique code in the SELIC system
        - ISIN: International Securities Identification Number
        - IssueDate: Date when the security was issued
        - Maturity: Security's maturity date
        - TradeCount: Number of trades executed
        - TradeQuantity: Quantity traded
        - TradeValue: Total value traded
        - AvgPrice: Average price
        - AvgRate: Average rate
        And additional trading metrics like min/max prices and rates.

    Examples:
        >>> from pyield import bc
        >>> df = bc.tpf_trades("07-01-2025")  # Returns all trades for January 2025
    """
    url = _build_download_url(target_date)
    df = _fetch_data_from_url(url)
    df = df.rename(columns=COLUMN_MAPPING)
    return df
