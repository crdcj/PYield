import logging

import polars as pl
import polars.selectors as cs

from pyield import bday
from pyield.b3._contracts import normalizar_codigos_contrato
from pyield.b3.futures.common import expr_dv01
from pyield.b3.intraday_derivatives import fetch_intraday_derivatives
from pyield.fwd import forwards

logger = logging.getLogger(__name__)


def intraday(codigo_contrato: str | list[str]) -> pl.DataFrame:
    """Busca os dados intraday mais recentes da B3.

    Os dados intraday da fonte possuem atraso aproximado de 15 minutos.
    A coluna ``LastUpdate`` reflete essa defasagem ao usar o horário atual
    menos 15 minutos.

    Args:
        codigo_contrato: Código base do contrato futuro na B3, ou lista de
            códigos.

    Returns:
        DataFrame Polars com dados intraday processados.

    Output Columns:
        * data_referencia (Date): data de negociação.
        * atualizado_as (Datetime): data e hora a que o dado se refere (com atraso de 15 min).
        * codigo_negociacao (String): código de negociação na B3.
        * data_vencimento (Date): data de vencimento do contrato.
        * dias_uteis (Int64): dias úteis até o vencimento.
        * dias_corridos (Int64): dias corridos até o vencimento.
        * contratos_abertos (Int64): contratos em aberto.
        * numero_negocios (Int64): número de negócios.
        * volume_negociado (Int64): quantidade de contratos negociados.
        * volume_financeiro (Float64): volume financeiro bruto.
        * dv01 (Float64): variação no preço para 1bp de taxa (apenas DI1).
        * preco_ultimo (Float64): último preço calculado (apenas DI1/DAP).
        * taxa_ajuste_anterior (Float64): taxa de ajuste do dia anterior.
        * taxa_limite_minimo (Float64): limite mínimo de variação (taxa).
        * taxa_limite_maximo (Float64): limite máximo de variação (taxa).
        * taxa_abertura (Float64): taxa de abertura.
        * taxa_minima (Float64): taxa mínima negociada.
        * taxa_media (Float64): taxa média negociada.
        * taxa_maxima (Float64): taxa máxima negociada.
        * taxa_oferta_compra (Float64): melhor oferta de compra (taxa, opcional).
        * taxa_oferta_venda (Float64): melhor oferta de venda (taxa, opcional).
        * taxa_ultima (Float64): última taxa negociada.
        * taxa_forward (Float64): taxa a termo (apenas DI1/DAP).
    """
    codigos = normalizar_codigos_contrato(codigo_contrato)
    if not codigos:
        return pl.DataFrame()

    dfs = [_intraday_contrato(c) for c in codigos]
    dfs = [df for df in dfs if not df.is_empty()]
    if not dfs:
        return pl.DataFrame()
    if len(dfs) == 1:
        return dfs[0]
    return pl.concat(dfs, how="diagonal_relaxed").sort("codigo_negociacao")


def _intraday_contrato(codigo_contrato: str) -> pl.DataFrame:
    """Busca e processa dados intraday de um único contrato."""
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
        df.filter(pl.col("codigo_mercado") == "FUT")
        .rename(
            {
                "preco_limite_minimo": "taxa_limite_minimo",
                "preco_ajuste_anterior": "taxa_ajuste_anterior",
                "preco_limite_maximo": "taxa_limite_maximo",
                "preco_abertura": "taxa_abertura",
                "preco_minimo": "taxa_minima",
                "preco_maximo": "taxa_maxima",
                "preco_medio": "taxa_media",
                "preco_ultimo": "taxa_ultima",
                "preco_oferta_compra": "taxa_oferta_compra",
                "preco_oferta_venda": "taxa_oferta_venda",
            },
            strict=False,
        )
        .sort("data_vencimento")
    )


def _processar_df_intraday(df: pl.DataFrame, codigo_contrato: str) -> pl.DataFrame:
    data_negociacao = bday.last_business_day()
    df = df.with_columns(
        cs.contains("taxa_").truediv(100).round(5),
        data_referencia=data_negociacao,
        dias_corridos=(pl.col("data_vencimento") - data_negociacao).dt.total_days(),
        dias_uteis=bday.count_expr(data_negociacao, "data_vencimento"),
    )

    if codigo_contrato in {"DI1", "DAP"}:
        taxa_fwd = forwards(bdays=df["dias_uteis"], rates=df["taxa_ultima"])
        anos_uteis = pl.col("dias_uteis") / 252
        preco_ultimo = 100_000 / ((1 + pl.col("taxa_ultima")) ** anos_uteis)
        df = df.with_columns(preco_ultimo=preco_ultimo.round(2), taxa_forward=taxa_fwd)

    if codigo_contrato == "DI1":
        df = df.with_columns(
            dv01=expr_dv01("dias_uteis", "taxa_ultima", "preco_ultimo")
        )

    return df.filter(pl.col("dias_corridos") > 0)


def _selecionar_e_reordenar_colunas_intraday(df: pl.DataFrame) -> pl.DataFrame:
    todas_colunas = [
        "data_referencia",
        "atualizado_as",
        "codigo_negociacao",
        "data_vencimento",
        "dias_uteis",
        "dias_corridos",
        "contratos_abertos",
        "numero_negocios",
        "volume_negociado",
        "volume_financeiro",
        "dv01",
        "preco_ultimo",
        "taxa_ajuste_anterior",
        "taxa_limite_minimo",
        "taxa_limite_maximo",
        "taxa_abertura",
        "taxa_minima",
        "taxa_media",
        "taxa_maxima",
        "taxa_oferta_compra",
        "taxa_oferta_venda",
        "taxa_ultima",
        "taxa_forward",
    ]
    colunas_reordenadas = [coluna for coluna in todas_colunas if coluna in df.columns]
    return df.select(colunas_reordenadas)
