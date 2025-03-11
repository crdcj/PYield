import io
import logging
from typing import Literal

import pandas as pd
import requests

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


def _fetch_last_ima() -> pd.DataFrame:
    r = requests.get(LAST_IMA_URL)
    r.raise_for_status()
    r.encoding = "latin1"
    text = r.text.split("2@COMPOSIÇÃO DE CARTEIRA")[1].strip()
    string_io_buffer = io.StringIO(text)

    df = pd.read_csv(
        string_io_buffer,
        sep="@",
        decimal=",",
        thousands=".",
        dtype_backend="numpy_nullable",
        na_values="--",
    )

    return df


def _process_last_ima(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=IMA_COL_MAPPING)[IMA_COL_MAPPING.values()]
    df["IndicativeRate"] = (df["IndicativeRate"] / 100).round(6)
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
    df["Maturity"] = pd.to_datetime(df["Maturity"], format="%d/%m/%Y")
    df["MarketQuantity"] = (1000 * df["MarketQuantity"]).astype("Int64")
    df["MarketValue"] = (1000 * df["MarketValue"]).round(0).astype("Int64")
    df["Price"] = df["Price"].round(6)
    # Duration is in business days, convert to years
    df["Duration"] /= 252
    mduration = df["Duration"] / (1 + df["IndicativeRate"])
    df["DV01"] = 0.0001 * mduration * df["Price"]
    # LFT DV01 is zero
    df["DV01"] = df["DV01"].where(df["BondType"] != "LFT", 0)
    # Since MarketDV01 is the total stock value, we round them to integer
    df["MarketDV01"] = (df["DV01"] * df["MarketQuantity"]).round(0).astype("Int64")

    return df


def _reorder_last_ima(df: pd.DataFrame) -> pd.DataFrame:
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
    return df[col_order].reset_index(drop=True)


def last_ima(ima_type: ima_types | None = None) -> pd.DataFrame:
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
        pd.DataFrame: A DataFrame containing the IMA data.

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
        df = _fetch_last_ima()
        df = _process_last_ima(df)
        df = _reorder_last_ima(df)
        if ima_type is not None:
            df = df.query("IMAType == @ima_type").reset_index(drop=True)
        df = df.sort_values(["IMAType", "BondType", "Maturity"]).reset_index(drop=True)
        return df
    except Exception as e:
        logger.exception(f"Error fetching or processing the last IMA data: {e}")
        return pd.DataFrame()
