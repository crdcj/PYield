import io
import logging
from typing import Literal

import pandas as pd
import requests

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

LAST_IMA_COL_MAPPING = {
    # "2",
    "Data de Referência": "Date",
    "INDICE": "IMAType",
    "Títulos": "BondType",
    "Data de Vencimento": "Maturity",
    "Código SELIC": "SelicCode",
    "Código ISIN": "ISIN",
    "Taxa Indicativa (% a.a.)": "IndicativeRate",
    "PU (R$)": "Price",
    # "PU de Juros (R$)": "InterestPrice",
    "Quantidade (1.000 títulos)": "MarketQuantity",
    "Quantidade Teórica (1.000 títulos)": "TheoreticalQuantity",
    "Carteira a Mercado (R$ mil)": "MarketValue",
    "Peso (%)": "Weight",
    "Prazo (d.u.)": "BDToMat",
    "Duration (d.u.)": "Duration",
    # "Número de Operações *": "NumberOfOperations",
    # "Quant. Negociada (1.000 títulos) *": "NegotiatedQuantity",
    # "Valor Negociado (R$ mil) *": "NegotiatedValue",
    "PMR": "PMR",  # Prazmo médio de repactuação
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
    )

    return df


def _process_last_ima(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=LAST_IMA_COL_MAPPING)[LAST_IMA_COL_MAPPING.values()]
    df["IndicativeRate"] = (df["IndicativeRate"] / 100).round(6)
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
    df["Maturity"] = pd.to_datetime(df["Maturity"], format="%d/%m/%Y")
    df["MarketQuantity"] = (1000 * df["MarketQuantity"]).astype("Int64")
    df["Price"] = df["Price"].round(6)
    # Duration is in business days, convert to years
    df["Duration"] /= 252
    mduration = df["Duration"] / (1 + df["IndicativeRate"])
    dv01 = 0.0001 * df["Price"] * mduration * df["MarketQuantity"]
    # Since DV01 and MarketValue are total stock values, we round them to integers
    df["MarketDV01"] = dv01.round(0).astype("Int64")
    df["MarketValue"] = (1000 * df["MarketValue"]).round(0).astype("Int64")
    # LFT DV01 is zero
    df["MarketDV01"] = df["MarketDV01"].where(df["BondType"] != "LFT", 0)
    return df


def _reorder_last_ima(df: pd.DataFrame) -> pd.DataFrame:
    col_order = [
        "Date",
        "IMAType",
        "BondType",
        "Maturity",
        "SelicCode",
        "ISIN",
        "Weight",
        "IndicativeRate",
        "Price",
        "BDToMat",
        "Duration",
        "PMR",
        "Convexity",
        "TheoreticalQuantity",
        "MarketDV01",
        "MarketQuantity",
        "MarketValue",
    ]
    return df[col_order].reset_index(drop=True)


def ima(ima_type: ima_types | None = None) -> pd.DataFrame:
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
        pd.DataFrame: A DataFrame containing the IMA data with the following columns:
            - Date: Reference date of the data.
            - Index: IMA index.
            - BondType: Type of bond.
            - Maturity: Bond maturity date.
            - SelicCode: Code representing the SELIC rate.
            - ISIN: International Securities Identification Number.
            - Price: Bond price.
            - Weight: Weight of the bond in the index.
            - BDToMat: Business days to maturity.
            - Duration: Duration of the bond in years.
            - PMR: Average repurchase term.
            - Convexity: Convexity of the bond.
            - TheoreticalQuantity: Theoretical quantity.
            - MarketDV01: Market DV01 in R$.
            - MarketQuantity: Market quantity.
            - MarketValue: Market value in R$.


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
        logging.exception(f"Error fetching or processing the last IMA data: {e}")
        return pd.DataFrame()
