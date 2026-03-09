import logging

import polars as pl
import requests

from pyield import clock
from pyield._internal.retry import retry_padrao

URL_BASE_INTRADAY = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation"

logger = logging.getLogger(__name__)


@retry_padrao
def _buscar_json_intraday(codigo_contrato: str) -> list[dict]:
    url = f"{URL_BASE_INTRADAY}/{codigo_contrato}"
    cabecalhos = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"  # noqa: E501
    }
    resposta = requests.get(url, headers=cabecalhos, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "utf-8"

    if "Quotation not available" in resposta.text:
        data_log = clock.now().strftime("%d-%m-%Y %H:%M")
        logger.warning("Sem dados intraday para %s em %s.", codigo_contrato, data_log)
        return []

    return resposta.json()["Scty"]


def _converter_json_intraday(dados_json: list[dict]) -> pl.DataFrame:
    if not dados_json:
        return pl.DataFrame()
    return pl.json_normalize(dados_json)


def _processar_colunas_intraday(df: pl.DataFrame) -> pl.DataFrame:
    df.columns = [
        c.replace("SctyQtn.", "").replace("asset.AsstSummry.", "") for c in df.columns
    ]

    mapa_renomeacao = {
        "symb": "TickerSymbol",
        "desc": "Description",
        "mkt.cd": "MarketCode",
        "asset.code": "AssetCode",
        "bottomLmtPric": "MinLimitValue",
        "prvsDayAdjstmntPric": "PrevSettlementValue",
        "topLmtPric": "MaxLimitValue",
        "opngPric": "OpenValue",
        "minPric": "MinValue",
        "maxPric": "MaxValue",
        "avrgPric": "AvgValue",
        "curPrc": "LastValue",
        "grssAmt": "FinancialVolume",
        "mtrtyCode": "ExpirationDate",
        "opnCtrcts": "OpenContracts",
        "tradQty": "TradeCount",
        "traddCtrctsQty": "TradeVolume",
        "buyOffer.price": "BestAskValue",
        "sellOffer.price": "BestBidValue",
    }
    colunas_disponiveis = [col for col in mapa_renomeacao if col in df.columns]
    return df.select(colunas_disponiveis).rename(mapa_renomeacao, strict=False)


def fetch_derivative_quotation(codigo_contrato: str) -> pl.DataFrame:
    """Busca cotações intraday brutas de derivativos da B3.

    Faz a chamada ao endpoint ``DerivativeQuotation`` e devolve um DataFrame
    padronizado, sem enriquecimento de regra de negócio.

    As colunas de cotação e limites são retornadas com sufixo ``Value``.
    Filtros por mercado (ex.: apenas ``FUT``), normalização semântica
    (``Rate``/``Price``) e cálculos derivados devem ser feitos no módulo
    consumidor.

    Args:
        codigo_contrato: Código base do derivativo na B3.

    Returns:
        DataFrame Polars com o payload normalizado da API.
    """
    dados_json = _buscar_json_intraday(codigo_contrato)
    if not dados_json:
        return pl.DataFrame()

    return (
        _converter_json_intraday(dados_json)
        .pipe(_processar_colunas_intraday)
        .with_columns(
            ExpirationDate=pl.col("ExpirationDate").str.to_date(
                format="%Y-%m-%d", strict=False
            )
        )
        .drop_nulls(subset=["ExpirationDate"])
        .sort("MarketCode", "TickerSymbol")
    )
