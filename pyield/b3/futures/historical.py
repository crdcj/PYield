import datetime as dt
from collections.abc import Sequence
from typing import Literal

import polars as pl

from pyield import bday
from pyield._internal.data_cache import obter_dataset_cacheado
from pyield.b3._contracts import normalizar_codigos_contrato
from pyield.b3.futures.common import adicionar_vencimento, expr_dv01
from pyield.b3.price_report import fetch_price_report
from pyield.fwd import forwards

# Lista de contratos que negociam por taxa (juros/cupom).
# Nestes contratos, as colunas OHLC são taxas e precisam ser divididas por 100.
CONTRATOS_TAXA = {"DI1", "DAP", "DDI", "FRC", "FRO"}


def historical(
    data: dt.date,
    codigo_contrato: str | Sequence[str] | pl.Series,
    tipo_fonte: Literal["PR", "SPR"] = "SPR",
) -> pl.DataFrame:
    """Busca histórico de futuros priorizando o dataset PR cacheado."""
    codigos = normalizar_codigos_contrato(codigo_contrato)
    if not codigos:
        return pl.DataFrame()

    dataframes: list[pl.DataFrame] = []
    codigos_sem_cache: list[str] = []

    for codigo in codigos:
        df_cache = _carregar_pr_por_data(data, codigo)
        if df_cache.is_empty():
            codigos_sem_cache.append(codigo)
        else:
            dataframes.append(df_cache)

    if codigos_sem_cache:
        for codigo in codigos_sem_cache:
            df_bruto = fetch_price_report(
                date=data,
                contract_code=codigo,
                source_type=tipo_fonte,
            )
            if df_bruto.is_empty():
                continue

            df_bruto = adicionar_vencimento(df_bruto, codigo, "TickerSymbol")
            df = _enriquecer_dados(df_bruto, codigo)
            df_saida = _selecionar_colunas_saida(df)
            if "ExpirationDate" in df_saida.columns:
                df_saida = df_saida.sort("ExpirationDate")
            dataframes.append(df_saida)

    if not dataframes:
        return pl.DataFrame()

    if len(dataframes) == 1:
        return dataframes[0]

    df_resultado = pl.concat(dataframes, how="diagonal_relaxed")
    colunas_ordenacao = [
        coluna
        for coluna in ["TradeDate", "TickerSymbol", "ExpirationDate"]
        if coluna in df_resultado.columns
    ]
    if not colunas_ordenacao:
        return df_resultado

    return df_resultado.sort(*colunas_ordenacao)


def _carregar_pr_por_data(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca o histórico de futuros no dataset PR cacheado para a data informada."""
    return carregar_pr([data], codigo_contrato)


def carregar_pr(datas: list[dt.date], codigo_contrato: str) -> pl.DataFrame:
    """Carrega histórico de futuros do dataset PR para uma lista de datas."""
    if not datas:
        return pl.DataFrame()

    df = obter_dataset_cacheado("pr")
    df = _filtrar_e_renomear(df, datas, codigo_contrato)
    if df.is_empty():
        return pl.DataFrame()

    df = adicionar_vencimento(df, codigo_contrato, coluna_ticker="TickerSymbol")
    df = _enriquecer_dados(df, codigo_contrato)
    df = _selecionar_colunas_saida(df)

    return df.sort("TradeDate", "ExpirationDate")


def listar_datas_disponiveis(codigo_contrato: str) -> pl.Series:
    """Lista datas disponíveis no dataset PR para um contrato futuro."""
    return (
        obter_dataset_cacheado("pr")
        .filter(pl.col("TickerSymbol").str.starts_with(codigo_contrato))
        .get_column("TradeDate")
        .drop_nulls()
        .unique()
        .sort()
        .alias("TradeDate")
    )


def _filtrar_e_renomear(
    df: pl.DataFrame, datas: list[dt.date], codigo_contrato: str
) -> pl.DataFrame:
    return df.filter(
        pl.col("TradeDate").is_in(datas),
        pl.col("TickerSymbol").str.starts_with(codigo_contrato),
    )


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
            DV01=expr_dv01("BDaysToExp", "SettlementRate", "SettlementPrice")
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
