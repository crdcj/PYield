import datetime as dt
import logging

import polars as pl
import polars.selectors as cs

from pyield import bday, clock
from pyield.b3._validar_pregao import data_negociacao_valida
from pyield.b3.futures.common import expr_dv01
from pyield.b3.intraday_derivatives import fetch_intraday_derivatives
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

    Args:
        codigo_contrato: Código base do contrato futuro na B3.

    Returns:
        DataFrame Polars com dados intraday processados.

    Output Columns:
        - TradeDate (Date): Data de negociação.
        - LastUpdate (Datetime): Horário da última atualização (com atraso de 15 min).
        - TickerSymbol (String): Código do ticker na B3.
        - ExpirationDate (Date): Data de vencimento do contrato.
        - BDaysToExp (Int64): Dias úteis até o vencimento.
        - DaysToExp (Int64): Dias corridos até o vencimento.
        - OpenContracts (Int64): Contratos em aberto.
        - TradeCount (Int64): Número de negócios.
        - TradeVolume (Int64): Quantidade de contratos negociados.
        - FinancialVolume (Float64): Volume financeiro bruto.
        - DV01 (Float64): Variação no preço para 1bp de taxa (apenas DI1).
        - LastPrice (Float64): Último preço calculado (apenas DI1/DAP).
        - PrevSettlementRate (Float64): Taxa de ajuste do dia anterior.
        - MinLimitRate (Float64): Limite mínimo de variação (taxa).
        - MaxLimitRate (Float64): Limite máximo de variação (taxa).
        - OpenRate (Float64): Taxa de abertura.
        - MinRate (Float64): Taxa mínima negociada.
        - AvgRate (Float64): Taxa média negociada.
        - MaxRate (Float64): Taxa máxima negociada.
        - BuyOfferRate (Float64): Melhor oferta de compra (taxa, opcional).
        - SellOfferRate (Float64): Melhor oferta de venda (taxa, opcional).
        - LastRate (Float64): Última taxa negociada.
        - ForwardRate (Float64): Taxa a termo (apenas DI1/DAP).
    """
    try:
        df_bruto = fetch_intraday_derivatives(codigo_contrato)
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
    return (
        df.filter(pl.col("MarketCode") == "FUT")
        .rename(
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
        )
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
