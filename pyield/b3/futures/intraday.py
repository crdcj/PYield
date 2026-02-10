"""
Exemplo de JSON da API da B3 para o contrato DI1:
[
    {'SctyQtn': {
        'bottomLmtPric': 12.43,
        'prvsDayAdjstmntPric': 13.396,
        'topLmtPric': 14.675,
        'opngPric': 13.37,
        'minPric': 13.37,
        'maxPric': 13.37,
        'avrgPric': 13.37,
        'curPrc': 13.37},
        'asset': {
            'AsstSummry': {
                'grssAmt': 1657811.68,
                'mtrtyCode': '2030-04-01',
                'opnCtrcts': 36457,
                'tradQty': 7,
                'traddCtrctsQty': 29},
                'code': 'DI1'
            },
            'buyOffer': {'price': 13.38},
            'mkt': {'cd': 'FUT'},
            'sellOffer': {'price': 13.395},
            'symb': 'DI1J30',
            'desc': 'DI DE 1 DIA'},
    {'SctyQtn': {...
"""

import datetime as dt
import logging

import polars as pl
import polars.selectors as cs
import requests

from pyield import bday, clock
from pyield.fwd import forwards
from pyield._internal.retry import retry_padrao

URL_BASE = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation"


# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
HORA_INICIO_INTRADAY = dt.time(9, 16)

logger = logging.getLogger(__name__)


@retry_padrao
def _buscar_json(codigo_contrato: str) -> list[dict]:
    url = f"{URL_BASE}/{codigo_contrato}"
    cabecalhos = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"  # noqa: E501
    }
    resposta = requests.get(url, headers=cabecalhos, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "utf-8"

    # Verifica se a resposta contém os dados esperados
    if "Quotation not available" in resposta.text or "curPrc" not in resposta.text:
        data_log = clock.now().strftime("%d-%m-%Y %H:%M")
        logger.warning(
            "Sem dados intraday para %s em %s.",
            codigo_contrato,
            data_log,
        )
        return []

    return resposta.json()["Scty"]


def _converter_json(dados_json: list[dict]) -> pl.DataFrame:
    if not dados_json:
        return pl.DataFrame()
    # Normalize JSON response into a flat table
    return pl.json_normalize(dados_json)


def _processar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    df.columns = [
        c.replace("SctyQtn.", "").replace("asset.AsstSummry.", "") for c in df.columns
    ]

    mapa_renomeacao = {
        "symb": "TickerSymbol",
        # "desc": "Description",
        # "asset.code": "AssetCode",
        # "mkt.cd": "MarketCode",
        "bottomLmtPric": "MinLimitRate",
        "prvsDayAdjstmntPric": "PrevSettlementRate",
        "topLmtPric": "MaxLimitRate",
        "opngPric": "OpenRate",
        "minPric": "MinRate",
        "maxPric": "MaxRate",
        "avrgPric": "AvgRate",
        "curPrc": "LastRate",
        "grssAmt": "FinancialVolume",
        "mtrtyCode": "ExpirationDate",
        "opnCtrcts": "OpenContracts",
        "tradQty": "TradeCount",
        "traddCtrctsQty": "TradeVolume",
        "buyOffer.price": "LastAskRate",
        "sellOffer.price": "LastBidRate",
    }
    df = df.select(mapa_renomeacao.keys()).rename(mapa_renomeacao, strict=False)
    return df


def _preprocessar_df(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.with_columns(
            pl.col("ExpirationDate").str.to_date(format="%Y-%m-%d", strict=False)
        )
        .drop_nulls(subset=["ExpirationDate"])
        .filter(pl.col("TickerSymbol") != "DI1D")  # Remove contrato dummy da API
        .sort("ExpirationDate")
    )
    return df


def _processar_df(df: pl.DataFrame, codigo_contrato: str) -> pl.DataFrame:
    data_negociacao = bday.last_business_day()
    df = df.with_columns(
        # Remove percentage in all rate columns
        cs.contains("Rate").truediv(100).round(5),
        TradeDate=data_negociacao,
        LastUpdate=clock.now() - dt.timedelta(minutes=15),
        DaysToExp=(pl.col("ExpirationDate") - data_negociacao).dt.total_days(),
    )

    df = df.with_columns(BDaysToExp=bday.count_expr(data_negociacao, "ExpirationDate"))

    if codigo_contrato in {"DI1", "DAP"}:  # Adiciona LastPrice para DI1 e DAP
        taxa_fwd = forwards(bdays=df["BDaysToExp"], rates=df["LastRate"])
        anos_uteis = pl.col("BDaysToExp") / 252
        ultimo_preco = 100_000 / ((1 + pl.col("LastRate")) ** anos_uteis)
        df = df.with_columns(
            LastPrice=ultimo_preco.round(2),
            ForwardRate=taxa_fwd,
        )

    if codigo_contrato == "DI1":  # Adiciona DV01 para DI1
        duracao = pl.col("BDaysToExp") / 252
        duracao_mod = duracao / (1 + pl.col("LastRate"))
        df = df.with_columns(DV01=0.0001 * duracao_mod * pl.col("LastPrice"))

    return df.filter(pl.col("DaysToExp") > 0)  # Remove expiring contracts


def _selecionar_e_reordenar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    todas_colunas = [
        "TradeDate",
        "LastUpdate",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        "OpenContracts",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "DV01",
        "LastPrice",
        "PrevSettlementRate",
        "MinLimitRate",
        "MaxLimitRate",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "LastAskRate",
        "LastBidRate",
        "LastRate",
        "ForwardRate",
    ]
    colunas_reordenadas = [col for col in todas_colunas if col in df.columns]
    return df.select(colunas_reordenadas)


def fetch_intraday_df(codigo_contrato: str) -> pl.DataFrame:
    """
    Busca os dados intraday mais recentes da B3.

    Returns:
        pl.DataFrame: DataFrame Polars contendo os dados intraday mais recentes.
    """
    try:
        dados_json = _buscar_json(codigo_contrato)
        if not dados_json:
            return pl.DataFrame()
        df = _converter_json(dados_json)
        df = _processar_colunas(df)
        df = _preprocessar_df(df)
        df = _processar_df(df, codigo_contrato)
        df = _selecionar_e_reordenar_colunas(df)
        return df
    except Exception as e:
        logger.exception(
            "CRITICAL: Pipeline falhou para %s. Erro: %s",
            codigo_contrato,
            e,
        )
        return pl.DataFrame()
