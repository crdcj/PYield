import datetime as dt
import logging

import polars as pl
import polars.selectors as cs
import requests

import pyield.b3.common as cm
from pyield import bday, clock
from pyield._internal.retry import retry_padrao
from pyield.fwd import forwards

URL_BASE_INTRADAY = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation"

# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
HORA_INICIO_INTRADAY = dt.time(9, 16)
# Pregão fecha às 18:00h, momento em que os dados consolidados começam a ser preparados.
HORA_FIM_INTRADAY = dt.time(18, 30)

logger = logging.getLogger(__name__)


def data_intraday_valida(data_verificacao: dt.date) -> bool:
    """Verifica se a data é elegível para consulta intraday."""
    if not cm.data_negociacao_valida(data_verificacao):
        return False

    return data_verificacao == clock.today()


def intraday(codigo_contrato: str) -> pl.DataFrame:
    """Busca os dados intraday mais recentes da B3."""
    try:
        dados_json = _buscar_json_intraday(codigo_contrato)
        if not dados_json:
            return pl.DataFrame()

        return (
            _converter_json_intraday(dados_json)
            .pipe(_processar_colunas_intraday)
            .pipe(_preprocessar_df_intraday)
            .pipe(_processar_df_intraday, codigo_contrato)
            .pipe(_selecionar_e_reordenar_colunas_intraday)
        )
    except Exception as erro:
        logger.exception(
            "CRITICAL: Pipeline intraday falhou para %s. Erro: %s",
            codigo_contrato,
            erro,
        )
        return pl.DataFrame()


@retry_padrao
def _buscar_json_intraday(codigo_contrato: str) -> list[dict]:
    url = f"{URL_BASE_INTRADAY}/{codigo_contrato}"
    cabecalhos = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"  # noqa: E501
    }
    resposta = requests.get(url, headers=cabecalhos, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "utf-8"

    if "Quotation not available" in resposta.text or "curPrc" not in resposta.text:
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
    return df.select(mapa_renomeacao.keys()).rename(mapa_renomeacao, strict=False)


def _preprocessar_df_intraday(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.col("ExpirationDate").str.to_date(format="%Y-%m-%d", strict=False)
        )
        .drop_nulls(subset=["ExpirationDate"])
        .filter(pl.col("TickerSymbol") != "DI1D")
        .sort("ExpirationDate")
    )


def _processar_df_intraday(df: pl.DataFrame, codigo_contrato: str) -> pl.DataFrame:
    data_negociacao = bday.last_business_day()
    df = df.with_columns(
        cs.contains("Rate").truediv(100).round(5),
        TradeDate=data_negociacao,
        LastUpdate=clock.now() - dt.timedelta(minutes=15),
        DaysToExp=(pl.col("ExpirationDate") - data_negociacao).dt.total_days(),
    )

    df = df.with_columns(BDaysToExp=bday.count_expr(data_negociacao, "ExpirationDate"))

    if codigo_contrato in {"DI1", "DAP"}:
        taxa_fwd = forwards(bdays=df["BDaysToExp"], rates=df["LastRate"])
        anos_uteis = pl.col("BDaysToExp") / 252
        ultimo_preco = 100_000 / ((1 + pl.col("LastRate")) ** anos_uteis)
        df = df.with_columns(LastPrice=ultimo_preco.round(2), ForwardRate=taxa_fwd)

    if codigo_contrato == "DI1":
        df = df.with_columns(DV01=cm.expr_dv01("BDaysToExp", "LastRate", "LastPrice"))

    return df.filter(pl.col("DaysToExp") > 0)


def _selecionar_e_reordenar_colunas_intraday(df: pl.DataFrame) -> pl.DataFrame:
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
    colunas_reordenadas = [coluna for coluna in todas_colunas if coluna in df.columns]
    return df.select(colunas_reordenadas)
