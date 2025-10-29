import io
import logging
from typing import Literal

import polars as pl
import requests

from pyield.retry import default_retry

logger = logging.getLogger(__name__)

ima_types = Literal[
    "IRF-M 1",
    "IRF-M 1+",
    "IRF-M",
    "IMA-B 5",
    "IMA-B 5+",
    "IMA-B",
    "IMA-S",
    "IMA-GERAL-EX-C",
    "IMA-GERAL",
]


IMA_SCHEMA = {
    "2": pl.Int64,  # Coluna inicial "2" que será descartada
    "Data de Referência": pl.String,
    "INDICE": pl.String,
    "Títulos": pl.String,
    "Data de Vencimento": pl.String,
    "Código SELIC": pl.Int64,
    "Código ISIN": pl.String,
    "Taxa Indicativa (% a.a.)": pl.Float64,
    "PU (R$)": pl.Float64,
    "PU de Juros (R$)": pl.Float64,
    "Quantidade (1.000 títulos)": pl.Float64,
    "Quantidade Teórica (1.000 títulos)": pl.Float64,
    "Carteira a Mercado (R$ mil)": pl.Float64,
    "Peso (%)": pl.Float64,
    "Prazo (d.u.)": pl.Int64,
    "Duration (d.u.)": pl.Int64,
    "Número de Operações *": pl.Int64,
    "Quant. Negociada (1.000 títulos) *": pl.Float64,
    "Valor Negociado (R$ mil) *": pl.Float64,
    "PMR": pl.Float64,  # Prazo médio de repactuação
    "Convexidade": pl.Float64,
}

IMA_COL_MAPPING = {
    # "2",
    "Data de Referência": "Date",
    "INDICE": "IMAType",
    "Títulos": "BondType",
    "Data de Vencimento": "Maturity",
    "Código SELIC": "SelicCode",
    "Código ISIN": "ISIN",
    "Taxa Indicativa (% a.a.)": "IndicativeRate",
    "PU (R$)": "Price",
    "PU de Juros (R$)": "InterestPrice",
    "Quantidade (1.000 títulos)": "MarketQuantity",
    "Quantidade Teórica (1.000 títulos)": "TheoreticalQuantity",
    "Carteira a Mercado (R$ mil)": "MarketValue",
    "Peso (%)": "Weight",
    "Prazo (d.u.)": "BDToMat",
    "Duration (d.u.)": "Duration",
    "Número de Operações *": "NumberOfOperations",
    "Quant. Negociada (1.000 títulos) *": "NegotiatedQuantity",
    "Valor Negociado (R$ mil) *": "NegotiatedValue",
    "PMR": "PMR",  # Prazo médio de repactuação
    "Convexidade": "Convexity",
}


LAST_IMA_URL = "https://www.anbima.com.br/informacoes/ima/arqs/ima_completo.txt"


@default_retry
def _fetch_last_ima_text() -> str:
    r = requests.get(LAST_IMA_URL, timeout=3)
    r.raise_for_status()
    r.encoding = "latin1"
    text = r.text.split("2@COMPOSIÇÃO DE CARTEIRA")[1].strip()
    return text


def _parse_df(text: str) -> pl.DataFrame:
    df = pl.read_csv(
        io.StringIO(text),
        separator="@",
        decimal_comma=True,
        null_values="--",
        schema=IMA_SCHEMA,
    )
    return df


def _process_df(df: pl.DataFrame) -> pl.DataFrame:
    dv01_expr = (
        0.0001 * pl.col("Price") * (pl.col("Duration") / (1 + pl.col("IndicativeRate")))
    )

    df = (
        df.rename(IMA_COL_MAPPING)
        .select(IMA_COL_MAPPING.values())
        .with_columns(
            pl.col("Date").str.strptime(pl.Date, "%d/%m/%Y"),
            pl.col("Maturity").str.strptime(pl.Date, "%d/%m/%Y"),
            (pl.col("IndicativeRate") / 100).round(6),
            (pl.col("MarketQuantity") * 1000).cast(pl.Int64),
            (pl.col("MarketValue") * 1000).round(2),
            (pl.col("NegotiatedQuantity") * 1000).cast(pl.Int64),
            (pl.col("NegotiatedValue") * 1000).round(2),
            pl.col("Duration") / 252,
        )
        .with_columns(
            dv01_expr.alias("DV01"),
        )
        .with_columns(
            (pl.col("DV01") * pl.col("MarketQuantity")).round(2).alias("MarketDV01"),
        )
    )
    return df


def _reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
    col_order = [
        "Date",
        "IMAType",
        "BondType",
        "Maturity",
        "SelicCode",
        "ISIN",
        "BDToMat",
        "Duration",
        "IndicativeRate",
        "Price",
        "InterestPrice",
        "DV01",
        "PMR",
        "Weight",
        "Convexity",
        "TheoreticalQuantity",
        "NumberOfOperations",
        "NegotiatedQuantity",
        "NegotiatedValue",
        "MarketDV01",
        "MarketQuantity",
        "MarketValue",
    ]
    return df.select(col_order)


def last_ima(ima_type: ima_types | None = None) -> pl.DataFrame:
    """
    Fetch and process the last IMA market data available from ANBIMA.

    This function processes the data into a structured DataFrame.
    It handles conversion of date formats, renames columns to English, and converts
    certain numeric columns to integer types. In the event of an error during data
    fetching or processing, an empty DataFrame is returned.

    Args:
        ima_type (str, optional): Type of IMA index to filter the data. If None, all
            IMA indexes are returned. Defaults to None.

    Returns:
        pl.DataFrame: A DataFrame containing the IMA data.

    DataFrame columns:
        - Date: reference date of the data.
        - IMAType: type of IMA index.
        - BondType: type of bond.
        - Maturity: bond maturity date.
        - SelicCode: bond code in the SELIC system.
        - ISIN: international Securities Identification Number.
        - BDToMat: business days to maturity.
        - Duration: duration of the bond in business years (252 days/year).
        - IndicativeRate: indicative rate.
        - Price: bond price.
        - InterestPrice: interest price.
        - DV01: DV01 in R$.
        - PMR: average repurchase term.
        - Weight: weight of the bond in the index.
        - Convexity: convexity of the bond.
        - TheoreticalQuantity: theoretical quantity.
        - NumberOfOperations: number of operations.
        - NegotiatedQuantity: negotiated quantity.
        - NegotiatedValue: negotiated value.
        - MarketQuantity: market quantity.
        - MarketDV01: market DV01 in R$.
        - MarketValue: market value in R$.

    Raises:
        Exception: Logs error and returns an empty DataFrame if any error occurs during
            fetching or processing.
    """
    try:
        ima_text = _fetch_last_ima_text()
        df = _parse_df(ima_text)
        df = _process_df(df)
        df = _reorder_columns(df)
        if ima_type is not None:
            df = df.filter(pl.col("IMAType") == ima_type)
        df = df.sort(["IMAType", "BondType", "Maturity"])
        return df
    except Exception as e:
        logger.exception(f"Error fetching or processing the last IMA data: {e}")
        return pl.DataFrame()
