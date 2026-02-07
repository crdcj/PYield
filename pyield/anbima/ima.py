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


# Única fonte de verdade para colunas do CSV: (novo_nome, tipo)
# Colunas com None no nome são descartadas após a leitura.
IMA_COLUMNS = {
    "2": (None, pl.Int64),
    "Data de Referência": ("Date", pl.String),
    "INDICE": ("IMAType", pl.String),
    "Títulos": ("BondType", pl.String),
    "Data de Vencimento": ("Maturity", pl.String),
    "Código SELIC": ("SelicCode", pl.Int64),
    "Código ISIN": ("ISIN", pl.String),
    "Taxa Indicativa (% a.a.)": ("IndicativeRate", pl.Float64),
    "PU (R$)": ("Price", pl.Float64),
    "PU de Juros (R$)": ("InterestPrice", pl.Float64),
    "Quantidade (1.000 títulos)": ("MarketQuantity", pl.Float64),
    "Quantidade Teórica (1.000 títulos)": ("TheoreticalQuantity", pl.Float64),
    "Carteira a Mercado (R$ mil)": ("MarketValue", pl.Float64),
    "Peso (%)": ("Weight", pl.Float64),
    "Prazo (d.u.)": ("BDToMat", pl.Int64),
    "Duration (d.u.)": ("Duration", pl.Int64),
    "Número de Operações *": ("NumberOfOperations", pl.Int64),
    "Quant. Negociada (1.000 títulos) *": ("NegotiatedQuantity", pl.Float64),
    "Valor Negociado (R$ mil) *": ("NegotiatedValue", pl.Float64),
    "PMR": ("PMR", pl.Float64),
    "Convexidade": ("Convexity", pl.Float64),
}

# Derivados automaticamente
IMA_SCHEMA = {k: v[1] for k, v in IMA_COLUMNS.items()}
IMA_COL_MAPPING = {k: v[0] for k, v in IMA_COLUMNS.items() if v[0] is not None}

FINAL_COLUMN_ORDER = [
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


LAST_IMA_URL = "https://www.anbima.com.br/informacoes/ima/arqs/ima_completo.txt"


@default_retry
def _fetch_last_ima_text() -> bytes:
    r = requests.get(LAST_IMA_URL, timeout=3)
    r.raise_for_status()
    r.encoding = "latin1"
    text = r.text.split("2@COMPOSIÇÃO DE CARTEIRA")[1].strip()
    return text.encode("utf-8")


def _parse_df(text: bytes) -> pl.DataFrame:
    df = pl.read_csv(
        text,
        separator="@",
        decimal_comma=True,
        null_values="--",
        schema_overrides=IMA_SCHEMA,
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
            pl.col("Date").str.to_date("%d/%m/%Y"),
            pl.col("Maturity").str.to_date("%d/%m/%Y"),
            pl.col("IndicativeRate").truediv(100).round(6),
            pl.col("MarketQuantity").mul(1000).cast(pl.Int64),
            pl.col("MarketValue").mul(1000).round(2),
            pl.col("NegotiatedQuantity").mul(1000).cast(pl.Int64),
            pl.col("NegotiatedValue").mul(1000).round(2),
            pl.col("Duration").truediv(252),
        )
        .with_columns(
            dv01_expr.alias("DV01"),
        )
        .with_columns(MarketDV01=(pl.col("DV01") * pl.col("MarketQuantity")).round(2))
        .select(FINAL_COLUMN_ORDER)
    )
    return df


def last_ima(ima_type: ima_types | None = None) -> pl.DataFrame:
    """Obtém os últimos dados de composição de carteira IMA disponíveis na ANBIMA.

    Busca e processa os dados do arquivo IMA completo publicado pela ANBIMA,
    retornando um DataFrame estruturado. Em caso de erro durante a busca ou
    processamento, retorna um DataFrame vazio.

    Args:
        ima_type (str, optional): Tipo de índice IMA para filtrar os dados.
            Se None, retorna todos os índices. Padrão é None.

    Returns:
        pl.DataFrame: DataFrame com os dados do IMA. Retorna um DataFrame vazio
            em caso de erro.

    Output Columns:
        * Date (Date): data de referência.
        * IMAType (String): tipo de índice IMA (ex: 'IMA-B', 'IRF-M').
        * BondType (String): tipo de título (ex: 'LTN', 'NTN-B').
        * Maturity (Date): data de vencimento do título.
        * SelicCode (Int64): código do título no sistema SELIC.
        * ISIN (String): código ISIN (International Securities Identification Number).
        * BDToMat (Int64): dias úteis até o vencimento.
        * Duration (Float64): duration do título em anos úteis (252 d.u./ano).
        * IndicativeRate (Float64): taxa indicativa em decimal (ex: 0.10 para 10%).
        * Price (Float64): preço unitário (PU) em R$.
        * InterestPrice (Float64): PU de juros em R$.
        * DV01 (Float64): DV01 em R$.
        * PMR (Float64): prazo médio de repactuação.
        * Weight (Float64): peso do título no índice (%).
        * Convexity (Float64): convexidade do título.
        * TheoreticalQuantity (Float64): quantidade teórica (em 1.000 títulos).
        * NumberOfOperations (Int64): número de operações.
        * NegotiatedQuantity (Int64): quantidade negociada (unidades).
        * NegotiatedValue (Float64): valor negociado em R$.
        * MarketDV01 (Float64): DV01 de mercado em R$.
        * MarketQuantity (Int64): quantidade em carteira (unidades).
        * MarketValue (Float64): valor de mercado em R$.

    Examples:
        >>> from pyield import anbima
        >>> df = anbima.last_ima()
        >>> df.columns[:6]
        ['Date', 'IMAType', 'BondType', 'Maturity', 'SelicCode', 'ISIN']
        >>> df.shape[1]
        22
        >>> df_imab = anbima.last_ima("IMA-B")
        >>> (df_imab["IMAType"] == "IMA-B").all()
        True
    """
    try:
        ima_text = _fetch_last_ima_text()
        df = _parse_df(ima_text)
        df = _process_df(df)
        if ima_type:
            df = df.filter(pl.col("IMAType") == ima_type)
        df = df.sort("IMAType", "BondType", "Maturity")
        return df
    except Exception as e:
        logger.exception(f"Error fetching or processing the last IMA data: {e}")
        return pl.DataFrame()
