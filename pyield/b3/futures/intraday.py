import datetime as dt
import logging

import polars as pl
import polars.selectors as cs

from pyield import bday, clock
from pyield.b3._validar_pregao import data_negociacao_valida
from pyield.b3.derivatives_intraday import fetch_derivative_quotation
from pyield.b3.futures.common import expr_dv01
from pyield.fwd import forwards

# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
HORA_INICIO_INTRADAY = dt.time(9, 16)
# Pregão fecha às 18:00h, momento em que os dados consolidados começam a ser preparados.
HORA_FIM_INTRADAY = dt.time(18, 30)

logger = logging.getLogger(__name__)


def data_intraday_valida(data_verificacao: dt.date) -> bool:
    """Verifica se a data é elegível para consulta intraday."""
    if not data_negociacao_valida(data_verificacao):
        return False

    return data_verificacao == clock.today()


def intraday(codigo_contrato: str) -> pl.DataFrame:
    """Busca os dados intraday mais recentes da B3.

    Os dados intraday da fonte possuem atraso aproximado de 15 minutos.
    A coluna ``LastUpdate`` reflete essa defasagem ao usar o horário atual
    menos 15 minutos.
    """
    try:
        df_bruto = fetch_derivative_quotation(codigo_contrato)
        if df_bruto.is_empty():
            return pl.DataFrame()

        return (
            df_bruto.pipe(_preprocessar_df_intraday)
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


def _preprocessar_df_intraday(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename(
        {
            "MinLimitValue": "MinLimitRate",
            "PrevSettlementValue": "PrevSettlementRate",
            "MaxLimitValue": "MaxLimitRate",
            "OpenValue": "OpenRate",
            "MinValue": "MinRate",
            "MaxValue": "MaxRate",
            "AvgValue": "AvgRate",
            "LastValue": "LastRate",
            "BuyOfferValue": "BuyOfferRate",
            "SellOfferValue": "SellOfferRate",
        },
        strict=False,
    ).sort("ExpirationDate")


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
        df = df.with_columns(DV01=expr_dv01("BDaysToExp", "LastRate", "LastPrice"))

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
        "BuyOfferRate",
        "SellOfferRate",
        "LastRate",
        "ForwardRate",
    ]
    colunas_reordenadas = [coluna for coluna in todas_colunas if coluna in df.columns]
    return df.select(colunas_reordenadas)
