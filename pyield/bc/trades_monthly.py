import logging

import pandas as pd

from pyield.date_converter import DateScalar, convert_input_dates
from pyield.retry import default_retry

logger = logging.getLogger(__name__)

BASE_URL = "https://www4.bcb.gov.br/pom/demab/negociacoes/download"

COLUMN_MAPPING = {
    "DATA MOV": "SettlementDate",
    "SIGLA": "BondType",
    "CODIGO": "SelicCode",
    "CODIGO ISIN": "ISIN",
    "EMISSAO": "IssueDate",
    "VENCIMENTO": "MaturityDate",
    "NUM DE OPER": "Trades",
    "QUANT NEGOCIADA": "Quantity",
    "VALOR NEGOCIADO": "Volume",
    "PU MIN": "MinPrice",
    "PU MED": "AvgPrice",
    "PU MAX": "MaxPrice",
    "PU LASTRO": "UnderlyingPrice",
    "VALOR PAR": "ParValue",
    "TAXA MIN": "MinRate",
    "TAXA MED": "AvgRate",
    "TAXA MAX": "MaxRate",
    "NUM OPER COM CORRETAGEM": "BrokerageTrades",
    "QUANT NEG COM CORRETAGEM": "BrokerageQuantity",
}


def _build_filename(target_date: DateScalar, extragoup: bool) -> str:
    """
    URL com todos os arquivos disponíveis:
    https://www4.bcb.gov.br/pom/demab/negociacoes/apresentacao.asp?frame=1

    Exemplo de URL para download:
    https://www4.bcb.gov.br/pom/demab/negociacoes/download/NegE202409.ZIP

    All Operations File format: NegTYYYYMM.ZIP
    Only Extra Group File format: NegEYYYYMM.ZIP
    """
    target_date = convert_input_dates(target_date)
    file_date = target_date.strftime("%Y%m")
    operation_acronym = "E" if extragoup else "T"
    return f"Neg{operation_acronym}{file_date}.ZIP"


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


def fpd_monthly_trades(
    target_date: DateScalar, extragroup: bool = False
) -> pd.DataFrame:
    """Fetches monthly secondary trading data for the domestic 'Federal Public Debt'
    (FPD) registered in the Brazilian Central Bank (BCB) Selic system.

    Downloads the monthly bond trading data from the Brazilian Central Bank (BCB)
    website for the month corresponding to the provided date. The data is downloaded
    as a ZIP file, extracted, and loaded into a Pandas DataFrame. The data contains
    all trades executed during the month, separated by each 'SettlementDate'.

    Args:
        target_date (DateScalar): The date for which the monthly trading data will be
            fetched. This date can be a string, datetime, or pandas Timestamp object.
            It will be converted to a pandas Timestamp object. Only the year and month
            of this date will be used to download the corresponding monthly file.
            extragroup (bool): If True, fetches only the trades that are considered
            'extragroup' (between different economic groups)".
            If False, fetches all trades. Default is False.
            Extragroup trades are those where the transferring counterparty's
            conglomerate is different from the receiving counterparty's conglomerate, or
            when at least one of the counterparties does not belong to a conglomerate.
            In the case of funds, the conglomerate considered is that of the
            administrator.

    Returns:
        pd.DataFrame: A DataFrame containing the bond trading data for the specified
            month.

    DataFrame columns:
        - SettlementDate: Date when the trade settled
        - BondType: Security type abbreviation
        - SelicCode: Unique code in the SELIC system
        - ISIN: International Securities Identification Number
        - IssueDate: Date when the security was issued
        - MaturityDate: Security's maturity date
        - Trades: Number of trades executed
        - Quantity: Quantity traded
        - Volume: Total value traded
        - AvgPrice: Average price
        - AvgRate: Average rate
        And additional trading metrics like min/max prices and rates.

    Examples:
        >>> from pyield import bc
        >>> df = bc.fpd_monthly_trades("07-01-2025")  # Returns all trades for Jan/2025
    """
    filename = _build_filename(target_date, extragroup)
    logger.info(f"Fetching FPD trades for {target_date} from BCB")
    url = f"{BASE_URL}/{filename}"
    df = _fetch_data_from_url(url)
    df = df.rename(columns=COLUMN_MAPPING)
    # Volume are empty in the original BCB file, so we calculate it
    df["Volume"] = (df["Quantity"] * df["AvgPrice"]).round(2)
    sort_cols = ["SettlementDate", "BondType", "MaturityDate"]
    df = df.sort_values(by=sort_cols).reset_index(drop=True)
    return df
