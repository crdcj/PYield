"""
Module to fetch monthly secondary trading data for the domestic 'Federal Public Debt'
(TPF - títulos públicos federais) registered in the Brazilian Central Bank (BCB) Selic system.
The data is downloaded as a ZIP file, extracted, and loaded into a Pandas DataFrame.
Example of the data format (first 3 lines):
DATA MOV  ; SIGLA; CODIGO; CODIGO ISIN ; EMISSAO   ; VENCIMENTO; NUM DE OPER; QUANT NEGOCIADA; VALOR NEGOCIADO; PU MIN        ; PU MED        ; PU MAX        ; PU LASTRO     ; VALOR PAR     ; TAXA MIN; TAXA MED; TAXA MAX; NUM OPER COM CORRETAGEM; QUANT NEG COM CORRETAGEM
02/09/2024; LFT  ; 210100; BRSTNCLF1RC4; 26/10/2018; 01/03/2025;          48;          100221;                ; 15288,00898200; 15292,57098100; 15302,77742100; 15285,54813387; 15288,23830700; -0,1897 ; -0,0565 ; 0,0032  ;                      20;                    16155
02/09/2024; LFT  ; 210100; BRSTNCLF1RD2; 08/03/2019; 01/09/2025;         101;          230120;                ; 15288,23830700; 15294,25937800; 15311,01778200; 15279,49187722; 15288,23830700; -0,1498 ; -0,0395 ; 0,0000  ;                      21;                    19059
02/09/2024; LFT  ; 210100; BRSTNCLF1RE0; 06/09/2019; 01/03/2026;          88;          512642;                ; 15286,63304100; 15288,20025100; 15292,77891300; 15268,60295396; 15288,23830700; -0,0198 ; 0,0002  ; 0,0071  ;                      27;                   121742
...
"""  # noqa: E501

import datetime as dt
import io
import logging
import zipfile as zf

import polars as pl
import requests
from requests.exceptions import HTTPError

from pyield.converters import convert_dates
from pyield.retry import default_retry
from pyield.types import DateLike, has_nullable_args

logger = logging.getLogger(__name__)

BASE_URL = "https://www4.bcb.gov.br/pom/demab/negociacoes/download"


# Using the original column names from the source file
TPF_TRADES_SCHEMA = {
    "DATA MOV": pl.String,  # Read as string, parse to date later for more control
    "SIGLA": pl.String,
    "CODIGO": pl.Int64,  # Selic unique code is integer
    "CODIGO ISIN": pl.String,
    "EMISSAO": pl.String,  # Read as string, parse to date later
    "VENCIMENTO": pl.String,  # Read as string, parse to date later
    "NUM DE OPER": pl.Int64,
    "QUANT NEGOCIADA": pl.Int64,
    "VALOR NEGOCIADO": pl.Float64,
    "PU MIN": pl.Float64,
    "PU MED": pl.Float64,
    "PU MAX": pl.Float64,
    "PU LASTRO": pl.Float64,
    "VALOR PAR": pl.Float64,
    "TAXA MIN": pl.Float64,
    "TAXA MED": pl.Float64,
    "TAXA MAX": pl.Float64,
    "NUM OPER COM CORRETAGEM": pl.Int64,
    "QUANT NEG COM CORRETAGEM": pl.Int64,
}

COLUMN_MAPPING = {
    "DATA MOV": "SettlementDate",
    "SIGLA": "BondType",
    "CODIGO": "SelicCode",
    "CODIGO ISIN": "ISIN",
    "EMISSAO": "IssueDate",
    "VENCIMENTO": "MaturityDate",
    "NUM DE OPER": "Trades",
    "QUANT NEGOCIADA": "Quantity",
    "VALOR NEGOCIADO": "Value",
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


def _build_filename(target_date: dt.date, extragroup: bool) -> str:
    """
    URL com todos os arquivos disponíveis:
    https://www4.bcb.gov.br/pom/demab/negociacoes/apresentacao.asp?frame=1

    Exemplo de URL para download:
    https://www4.bcb.gov.br/pom/demab/negociacoes/download/NegE202409.ZIP

    All Operations File format: NegTYYYYMM.ZIP
    Only Extra Group File format: NegEYYYYMM.ZIP
    """
    year_month = target_date.strftime("%Y%m")
    operation_acronym = "E" if extragroup else "T"
    return f"Neg{operation_acronym}{year_month}.ZIP"


def _build_file_url(target_date: dt.date, extragroup: bool) -> str:
    filename = _build_filename(target_date, extragroup)
    return f"{BASE_URL}/{filename}"


@default_retry
def _fetch_zip_from_url(file_url: str) -> bytes:
    response = requests.get(file_url, timeout=10)
    response.raise_for_status()
    return response.content


def _uncompress_zip(zip_content: bytes) -> io.BytesIO:
    with zf.ZipFile(io.BytesIO(zip_content), "r") as file_zip:
        # Lê o conteúdo do arquivo CSV para a memória como um objeto de bytes
        csv_bytes = file_zip.read(file_zip.namelist()[0])
        return io.BytesIO(csv_bytes)


def _read_dataframe_from_zip(buffer: io.BytesIO) -> pl.DataFrame:
    df = pl.read_csv(
        buffer,
        decimal_comma=True,
        encoding="latin1",
        separator=";",
        schema_overrides=TPF_TRADES_SCHEMA,
    )
    return df


def _process_df(df: pl.DataFrame) -> pl.DataFrame:
    date_cols = ["SettlementDate", "IssueDate", "MaturityDate"]
    df = (
        df.rename(COLUMN_MAPPING)
        .with_columns(
            pl.col(date_cols).str.strptime(pl.Date, format="%d/%m/%Y", strict=False),
            # Refazer o cálculo do valor pois ele vem vazio no arquivo
            Value=(pl.col("Quantity") * pl.col("AvgPrice")).round(2),
        )
        .sort("SettlementDate", "BondType", "MaturityDate")
    )
    return df


def tpf_monthly_trades(target_date: DateLike, extragroup: bool = False) -> pl.DataFrame:
    """Fetches monthly secondary trading data for the domestic 'Federal Public Debt'
    (TPF - títulos públicos federais) registered in the Brazilian Central Bank (BCB)
    Selic system.

    Downloads the monthly bond trading data from the Brazilian Central Bank (BCB)
    website for the month corresponding to the provided date. The data is downloaded
    as a ZIP file, extracted, and loaded into a Pandas DataFrame. The data contains
    all trades executed during the month, separated by each 'SettlementDate'.

    Args:
        target_date (DateLike): The date for which the monthly trading data will be
            fetched. This date can be a string, datetime, or pandas Timestamp object.
            It will be converted to a date object. Only the year and month
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
        pl.DataFrame: A DataFrame containing the bond trading data for the specified
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
        - Value: Value traded
        - AvgPrice: Average price
        - AvgRate: Average rate
        And additional trading metrics like min/max prices and rates.

    Examples:
        >>> from pyield import bc
        >>> # Fetches all trades for Jan/2025
        >>> bc.tpf_monthly_trades("07-01-2025", extragroup=True)
        shape: (1_019, 19)
        ┌────────────────┬──────────┬───────────┬──────────────┬───┬─────────┬─────────┬─────────────────┬───────────────────┐
        │ SettlementDate ┆ BondType ┆ SelicCode ┆ ISIN         ┆ … ┆ AvgRate ┆ MaxRate ┆ BrokerageTrades ┆ BrokerageQuantity │
        │ ---            ┆ ---      ┆ ---       ┆ ---          ┆   ┆ ---     ┆ ---     ┆ ---             ┆ ---               │
        │ date           ┆ str      ┆ i64       ┆ str          ┆   ┆ f64     ┆ f64     ┆ i64             ┆ i64               │
        ╞════════════════╪══════════╪═══════════╪══════════════╪═══╪═════════╪═════════╪═════════════════╪═══════════════════╡
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RC4 ┆ … ┆ 0.0132  ┆ 0.0906  ┆ 2               ┆ 9581              │
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RD2 ┆ … ┆ 0.0561  ┆ 0.101   ┆ 11              ┆ 42823             │
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RE0 ┆ … ┆ 0.0191  ┆ 0.0405  ┆ 19              ┆ 33330             │
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RF7 ┆ … ┆ 0.0304  ┆ 0.05    ┆ 10              ┆ 14583             │
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RG5 ┆ … ┆ 0.0697  ┆ 0.0935  ┆ 12              ┆ 51776             │
        │ …              ┆ …        ┆ …         ┆ …            ┆ … ┆ …       ┆ …       ┆ …               ┆ …                 │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF1P8 ┆ … ┆ null    ┆ null    ┆ 0               ┆ 0                 │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF1Q6 ┆ … ┆ null    ┆ null    ┆ 0               ┆ 0                 │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF204 ┆ … ┆ null    ┆ null    ┆ 12              ┆ 570000            │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF212 ┆ … ┆ null    ┆ null    ┆ 0               ┆ 0                 │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF238 ┆ … ┆ null    ┆ null    ┆ 4               ┆ 115000            │
        └────────────────┴──────────┴───────────┴──────────────┴───┴─────────┴─────────┴─────────────────┴───────────────────┘

    """  # noqa: E501
    if has_nullable_args(target_date):
        logger.warning("No target_date provided. Returning an empty DataFrame.")
        return pl.DataFrame()
    try:
        target_date = convert_dates(target_date)
        url = _build_file_url(target_date, extragroup)
        zip_content = _fetch_zip_from_url(url)
        extracted_file = _uncompress_zip(zip_content)
        df = _read_dataframe_from_zip(extracted_file)
        df = _process_df(df)

    except HTTPError as e:
        if e.response.status_code == 404:  # noqa
            msg = f"Resource not found (404) at {url}. Returning an empty DataFrame."
            logger.warning(msg)
            return pl.DataFrame()
        else:
            # Captures the full traceback for unexpected HTTP errors
            msg = f"Unexpected HTTP error ({e.code}) while accessing URL: {url}"
            logger.exception(msg)
            raise e

    except Exception:
        # Captures the full traceback for any other errors
        msg = f"An unexpected error occurred while processing data from {url}."
        logger.exception(msg)
        raise

    # LOG DE SUCESSO
    msg = f"Successfully processed data from {url}. Found {len(df)} records."
    logger.info(msg)

    return df
