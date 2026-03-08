import datetime as dt
import logging

import polars as pl

import pyield.b3.common as cm
from pyield import bday
from pyield._internal.data_cache import obter_dataset_cacheado
from pyield.b3.price_report import fetch_price_report
from pyield.fwd import forwards

# Lista de contratos que negociam por taxa (juros/cupom).
# Nestes contratos, as colunas OHLC são taxas e precisam ser divididas por 100.
CONTRATOS_TAXA = {"DI1", "DAP", "DDI", "FRC", "FRO"}

MAPA_RENOMEACAO_DATASET_PR = {
    "TradDt": "TradeDate",
    "TckrSymb": "TickerSymbol",
    "TradQty": "TradeCount",
    "FinInstrmQty": "TradeVolume",
    "NtlFinVol": "FinancialVolume",
    "OpnIntrst": "OpenContracts",
    "BestBidPric": "BestBidValue",
    "BestAskPric": "BestAskValue",
    "FrstPric": "OpenValue",
    "MinPric": "MinValue",
    "MaxPric": "MaxValue",
    "TradAvrgPric": "AvgValue",
    "LastPric": "CloseValue",
    "AdjstdQt": "SettlementPrice",
    "AdjstdQtTax": "SettlementRate",
    "AdjstdValCtrct": "AdjustedValueContract",
    "MaxTradLmt": "MaxLimitValue",
    "MinTradLmt": "MinLimitValue",
}

logger = logging.getLogger(__name__)


def historical(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca histórico de futuros priorizando o dataset PR cacheado."""
    df_cache = _carregar_pr_por_data(data, codigo_contrato)
    if not df_cache.is_empty():
        return df_cache

    try:
        return fetch_price_report(
            date=data, contract_code=codigo_contrato, source_type="SPR"
        )
    except Exception:
        return pl.DataFrame()


def _carregar_pr_por_data(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca o histórico de futuros no dataset PR cacheado para a data informada."""
    return carregar_pr([data], codigo_contrato)


def carregar_pr(datas: list[dt.date], codigo_contrato: str) -> pl.DataFrame:
    """Carrega histórico de futuros do dataset PR para uma lista de datas."""
    if not datas:
        return pl.DataFrame()

    try:
        df = obter_dataset_cacheado("pr")
        df = _filtrar_e_renomear_pr(df, datas, codigo_contrato)
        if df.is_empty():
            return pl.DataFrame()

        df = cm.adicionar_vencimento(df, codigo_contrato, coluna_ticker="TickerSymbol")
        df = _enriquecer_dados(df, codigo_contrato)
        df = _selecionar_colunas_saida(df)

        return df.sort("TradeDate", "ExpirationDate")
    except Exception as erro:
        logger.exception(
            "CRITICAL: Falha ao processar histórico no dataset PR do contrato %s para %s. Erro: %s",
            codigo_contrato,
            datas,
            erro,
        )
        return pl.DataFrame()


def listar_datas_disponiveis_pr(codigo_contrato: str) -> pl.Series:
    """Lista datas disponíveis no dataset PR para um contrato futuro."""
    return (
        obter_dataset_cacheado("pr")
        .filter(pl.col("TckrSymb").str.starts_with(codigo_contrato))
        .get_column("TradDt")
        .drop_nulls()
        .unique()
        .sort()
        .alias("TradeDate")
    )


def _filtrar_e_renomear_pr(
    df: pl.DataFrame, datas: list[dt.date], codigo_contrato: str
) -> pl.DataFrame:
    return df.filter(
        pl.col("TradDt").is_in(datas),
        pl.col("TckrSymb").str.starts_with(codigo_contrato),
    ).rename(MAPA_RENOMEACAO_DATASET_PR)


def _enriquecer_dados(df: pl.DataFrame, codigo_contrato: str) -> pl.DataFrame:
    df = df.with_columns(
        BDaysToExp=bday.count_expr("TradeDate", "ExpirationDate"),
        DaysToExp=(pl.col("ExpirationDate") - pl.col("TradeDate")).dt.total_days(),
    ).filter(pl.col("DaysToExp") > 0)

    eh_taxa = codigo_contrato in CONTRATOS_TAXA
    sufixo_destino = "Rate" if eh_taxa else "Price"

    colunas_renomear = [c for c in df.columns if c.endswith("Value")]
    mapa_renomeacao = {c: c.replace("Value", sufixo_destino) for c in colunas_renomear}
    df = df.rename(mapa_renomeacao)

    if eh_taxa:
        colunas_taxa = [c for c in df.columns if c.endswith("Rate")]
        df = df.with_columns(pl.col(colunas_taxa).truediv(100).round(6))

    if (
        codigo_contrato == "DI1"
        and "SettlementPrice" in df.columns
        and "SettlementRate" in df.columns
    ):
        df = df.with_columns(
            DV01=cm.expr_dv01("BDaysToExp", "SettlementRate", "SettlementPrice")
        )

    if codigo_contrato in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        df = df.with_columns(
            ForwardRate=forwards(bdays=df["BDaysToExp"], rates=df["SettlementRate"])
        )

    return df


def _selecionar_colunas_saida(df: pl.DataFrame) -> pl.DataFrame:
    ordem_preferida = [
        "TradeDate",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        "DV01",
        "OpenContracts",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "AdjustedValueContract",
        "MinLimitPrice",
        "MaxLimitPrice",
        "OpenPrice",
        "MinPrice",
        "MaxPrice",
        "AvgPrice",
        "ClosePrice",
        "BestBidPrice",
        "BestAskPrice",
        "SettlementPrice",
        "MinLimitRate",
        "MaxLimitRate",
        "OpenRate",
        "MinRate",
        "MaxRate",
        "AvgRate",
        "CloseRate",
        "BestBidRate",
        "BestAskRate",
        "SettlementRate",
        "ForwardRate",
    ]
    colunas_existentes = [c for c in ordem_preferida if c in df.columns]
    return df.select(colunas_existentes)
