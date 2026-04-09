import datetime as dt

import polars as pl
import polars.selectors as cs

from pyield import du
from pyield._internal.data_cache import obter_dataset_cacheado
from pyield.b3.futuro.contratos import (
    CONTRATOS_TAXA,
    adicionar_vencimento,
    expr_dv01,
)
from pyield.fwd import forwards

# Renomeação preco_* → taxa_* para contratos cotados por taxa.
# Bid/Ask são invertidos: BestBidPric (bid em PU) = menor taxa = venda de taxa;
# BestAskPric (ask em PU) = maior taxa = compra de taxa.
_PRECO_PARA_TAXA = {
    "preco_abertura": "taxa_abertura",
    "preco_minimo": "taxa_minima",
    "preco_maximo": "taxa_maxima",
    "preco_medio": "taxa_media",
    "preco_fechamento": "taxa_fechamento",
    "preco_ultima_oferta_compra": "taxa_ultima_oferta_venda",
    "preco_ultima_oferta_venda": "taxa_ultima_oferta_compra",
    "preco_limite_minimo": "taxa_limite_minimo",
    "preco_limite_maximo": "taxa_limite_maximo",
}

# Colunas de saída para contratos cotados por preço (DOL, WDO, IND, WIN).
_COLUNAS_CONTRATO_PRECO = (
    "data_referencia",
    "codigo_negociacao",
    "data_vencimento",
    "dias_uteis",
    "dias_corridos",
    "contratos_abertos",
    "numero_negocios",
    "volume_negociado",
    "volume_financeiro",
    "preco_limite_minimo",
    "preco_limite_maximo",
    "preco_abertura",
    "preco_minimo",
    "preco_maximo",
    "preco_medio",
    "preco_fechamento",
    "preco_ultima_oferta_compra",
    "preco_ultima_oferta_venda",
    "preco_ajuste",
)

# Colunas de saída para contratos cotados por taxa (DI1, DAP, DDI, FRC, FRO).
_COLUNAS_CONTRATO_TAXA = (
    "data_referencia",
    "codigo_negociacao",
    "data_vencimento",
    "dias_uteis",
    "dias_corridos",
    "dv01",
    "contratos_abertos",
    "numero_negocios",
    "volume_negociado",
    "volume_financeiro",
    "preco_ajuste",
    "taxa_limite_minimo",
    "taxa_limite_maximo",
    "taxa_abertura",
    "taxa_minima",
    "taxa_maxima",
    "taxa_media",
    "taxa_fechamento",
    "taxa_ultima_oferta_venda",
    "taxa_ultima_oferta_compra",
    "taxa_ajuste",
    "taxa_forward",
)

# Comprimento padrão de ticker de futuro na B3 (prefixo 3 + mês 1 + ano 2).
_COMPRIMENTO_TICKER = 6

# Normaliza o schema XML bruto da B3 para o padrão de colunas deste módulo.
_RENOMEAR_COLUNAS_PR = {
    "TradDt": "data_referencia",
    "TckrSymb": "codigo_negociacao",
    "OpnIntrst": "contratos_abertos",
    "TradQty": "numero_negocios",
    "FinInstrmQty": "volume_negociado",
    "NtlFinVol": "volume_financeiro",
    "BestBidPric": "preco_ultima_oferta_compra",
    "BestAskPric": "preco_ultima_oferta_venda",
    "FrstPric": "preco_abertura",
    "MinPric": "preco_minimo",
    "MaxPric": "preco_maximo",
    "TradAvrgPric": "preco_medio",
    "LastPric": "preco_fechamento",
    "AdjstdQt": "preco_ajuste",
    "AdjstdQtTax": "taxa_ajuste",
    "MaxTradLmt": "preco_limite_maximo",
    "MinTradLmt": "preco_limite_minimo",
}


def _obter_cache_filtrado(contrato: str) -> pl.DataFrame:
    """Carrega o dataset PR cacheado e filtra por contrato."""
    return obter_dataset_cacheado("futuro").filter(
        pl.col("TckrSymb").str.starts_with(contrato)
    )


def _enriquecer_dados(df: pl.DataFrame, contrato: str) -> pl.DataFrame:
    df = df.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
        dias_corridos=(
            pl.col("data_vencimento") - pl.col("data_referencia")
        ).dt.total_days(),
    ).filter(pl.col("dias_corridos") > 0)

    eh_taxa = contrato in CONTRATOS_TAXA
    if eh_taxa:
        df = df.rename(_PRECO_PARA_TAXA, strict=False)
        df = df.with_columns(cs.starts_with("taxa_").truediv(100).round(6))

    if contrato == "DI1":
        df = df.with_columns(
            dv01=expr_dv01("dias_uteis", "taxa_ajuste", "preco_ajuste")
        )

    if contrato in {"DI1", "DAP"}:
        df = df.with_columns(
            taxa_forward=forwards(
                dias_uteis=df["dias_uteis"],
                taxas=df["taxa_ajuste"],
                agrupar_por=df["data_referencia"],
            )
        )

    return df


def _selecionar_colunas_saida(df: pl.DataFrame, contrato: str) -> pl.DataFrame:
    if contrato in CONTRATOS_TAXA:
        colunas = _COLUNAS_CONTRATO_TAXA
    else:
        colunas = _COLUNAS_CONTRATO_PRECO
    return df.select(c for c in colunas if c in df.columns)


def _buscar_do_cache(datas: list[dt.date], contrato: str) -> pl.DataFrame:
    """Carrega histórico de futuros do dataset PR para uma lista de datas."""
    if not datas:
        return pl.DataFrame()

    df = _obter_cache_filtrado(contrato)
    df = df.filter(pl.col("TradDt").is_in(datas))
    if df.is_empty():
        return pl.DataFrame()

    return enriquecer(df, contrato)


def enriquecer(df: pl.DataFrame, contrato: str) -> pl.DataFrame:
    """Enriquece DataFrame bruto do Price Report (PR) da B3.

    Aceita um DataFrame com colunas no schema original da B3 (ex.:
    ``TradDt``, ``TckrSymb``). Filtra pelo contrato informado,
    adiciona data de vencimento, dias úteis/corridos e colunas
    derivadas (dv01, taxa_forward) conforme o contrato.

    Args:
        df: DataFrame com dados brutos do PR da B3.
        contrato: Contrato futuro (ex.: "DI1", "DOL").

    Returns:
        DataFrame Polars enriquecido e ordenado.
    """
    if df.is_empty():
        return pl.DataFrame()

    df = df.rename(_RENOMEAR_COLUNAS_PR)
    df = df.filter(
        pl.col("codigo_negociacao").str.starts_with(contrato),
        pl.col("codigo_negociacao").str.len_chars() == _COMPRIMENTO_TICKER,
    )
    if df.is_empty():
        return pl.DataFrame()
    df = adicionar_vencimento(df, contrato, coluna_ticker="codigo_negociacao")
    df = _enriquecer_dados(df, contrato)
    df = _selecionar_colunas_saida(df, contrato)

    return df.sort("data_referencia", "data_vencimento")


def historico(
    data: dt.date,
    contrato: str,
) -> pl.DataFrame:
    """Busca histórico de futuros no dataset PR cacheado.

    Args:
        data: Data de negociação.
        contrato: Contrato futuro na B3.

    Returns:
        DataFrame Polars com dados históricos de futuros.

    Output Columns:
        * data_referencia (Date): data de negociação.
        * codigo_negociacao (String): código de negociação na B3.
        * data_vencimento (Date): data de vencimento do contrato.
        * dias_uteis (Int64): dias úteis até o vencimento.
        * dias_corridos (Int64): dias corridos até o vencimento.
        * dv01 (Float64): variação no preço para 1bp de taxa (apenas DI1).
        * contratos_abertos (Int64): contratos em aberto.
        * numero_negocios (Int64): número de negócios.
        * volume_negociado (Int64): quantidade de contratos negociados.
        * volume_financeiro (Float64): volume financeiro bruto.
        * preco_limite_minimo (Float64): limite mínimo de variação (preço).
        * preco_limite_maximo (Float64): limite máximo de variação (preço).
        * preco_abertura (Float64): preço de abertura.
        * preco_minimo (Float64): preço mínimo negociado.
        * preco_maximo (Float64): preço máximo negociado.
        * preco_medio (Float64): preço médio negociado.
        * preco_fechamento (Float64): último preço negociado (last).
        * preco_ultima_oferta_compra (Float64): melhor preço de compra
          (bid) ao fim do pregão.
        * preco_ultima_oferta_venda (Float64): melhor preço de venda
          (ask) ao fim do pregão.
        * preco_ajuste (Float64): preço de ajuste.
        * taxa_limite_minimo (Float64): limite mínimo de variação (taxa).
        * taxa_limite_maximo (Float64): limite máximo de variação (taxa).
        * taxa_abertura (Float64): taxa de abertura.
        * taxa_minima (Float64): taxa mínima negociada.
        * taxa_maxima (Float64): taxa máxima negociada.
        * taxa_media (Float64): taxa média negociada.
        * taxa_fechamento (Float64): última taxa negociada (last).
        * taxa_ultima_oferta_venda (Float64): melhor taxa de venda
          (dar; bid em PU) no fim do pregão.
        * taxa_ultima_oferta_compra (Float64): melhor taxa de compra
          (tomar; ask em PU) no fim do pregão.
        * taxa_ajuste (Float64): taxa de ajuste.
        * taxa_forward (Float64): taxa a termo (apenas DI1/DAP).

    Notes:
        Usa exclusivamente o dataset PR cacheado no GitHub. Contratos
        disponíveis: DI1, DDI, FRC, FRO, DAP, DOL, WDO, IND, WIN.

        As colunas com prefixo ``preco_`` aparecem para contratos cotados
        por preço (ex.: DOL, IND). As com prefixo ``taxa_`` aparecem para
        contratos cotados por taxa (ex.: DI1, DAP, DDI, FRC, FRO). Nem
        todas as colunas estarão presentes em todos os contratos.

        ``*_fechamento`` é o último negócio realizado (last trade).
        ``*_ultima_oferta_*`` é o bid/ask ao fim do pregão — não
        representa negócio realizado e pode ser nulo. Para contratos de
        taxa (DI1, DAP, etc.), bid/ask são invertidos em relação ao PU:
        ``taxa_ultima_oferta_compra`` = maior taxa (ask em PU),
        ``taxa_ultima_oferta_venda`` = menor taxa (bid em PU).
    """
    return _buscar_do_cache([data], contrato)


def listar_datas_disponiveis(contrato: str) -> pl.Series:
    """Lista datas disponíveis no dataset PR para um contrato futuro."""
    return (
        _obter_cache_filtrado(contrato)
        .get_column("TradDt")
        .drop_nulls()
        .unique()
        .sort()
        .alias("data_referencia")
    )
