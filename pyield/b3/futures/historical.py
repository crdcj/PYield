import datetime as dt

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

# Renomeação preco_* → taxa_* para contratos cotados por taxa.
_PRECO_PARA_TAXA = {
    "preco_abertura": "taxa_abertura",
    "preco_minimo": "taxa_minima",
    "preco_maximo": "taxa_maxima",
    "preco_medio": "taxa_media",
    "preco_fechamento": "taxa_fechamento",
    "preco_ultima_oferta_compra": "taxa_ultima_oferta_compra",
    "preco_ultima_oferta_venda": "taxa_ultima_oferta_venda",
    "preco_limite_minimo": "taxa_limite_minimo",
    "preco_limite_maximo": "taxa_limite_maximo",
}

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


def _normalizar_colunas_pr(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({k: v for k, v in _RENOMEAR_COLUNAS_PR.items() if k in df.columns})


def _obter_pr_normalizado() -> pl.DataFrame:
    """Carrega o dataset PR e normaliza nomes de colunas."""
    df = obter_dataset_cacheado("futures")
    return _normalizar_colunas_pr(df)


def historical(
    data: dt.date,
    codigo_contrato: str | list[str],
    full_report: bool | None = None,
) -> pl.DataFrame:
    """Busca histórico de futuros priorizando o dataset PR cacheado.

    Args:
        data: Data de negociação.
        codigo_contrato: Código(s) do contrato futuro na B3.
        full_report: Se None (padrão), tenta SPR primeiro e PR como fallback.
            Se False, usa o simplified price report (SPR, ~2 KB).
            Se True, usa o price report completo (PR, ~2 MB).

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
        * taxa_ultima_oferta_compra (Float64): melhor taxa de compra
          (bid) no fim do pregão.
        * taxa_ultima_oferta_venda (Float64): melhor taxa de venda
          (ask) no fim do pregão.
        * taxa_ajuste (Float64): taxa de ajuste.
        * taxa_forward (Float64): taxa a termo (apenas DI1/DAP).

    Notes:
        As colunas com prefixo ``preco_`` aparecem para contratos cotados por
        preço (ex.: DOL, IND). As com prefixo ``taxa_`` aparecem para contratos
        cotados por taxa (ex.: DI1, DAP, DDI, FRC, FRO). Nem todas as colunas
        estarão presentes em todos os contratos.

        ``*_fechamento`` é o último negócio realizado (last trade).
        ``*_ultima_oferta_*`` é o bid/ask ao fim do pregão — não
        representa negócio realizado e pode ser nulo.
    """
    codigos = normalizar_codigos_contrato(codigo_contrato)
    if not codigos:
        return pl.DataFrame()

    dataframes: list[pl.DataFrame] = []
    codigos_sem_cache: list[str] = []

    for codigo in codigos:
        df_cache = _obter_futuros_pr([data], codigo)
        if df_cache.is_empty():
            codigos_sem_cache.append(codigo)
        else:
            dataframes.append(df_cache)

    if codigos_sem_cache:
        for codigo in codigos_sem_cache:
            df_bruto = _buscar_price_report(data, codigo, full_report)
            if df_bruto.is_empty():
                continue

            df_bruto = adicionar_vencimento(df_bruto, codigo, "codigo_negociacao")
            df = _enriquecer_dados(df_bruto, codigo)
            df_saida = _selecionar_colunas_saida(df)
            if "data_vencimento" in df_saida.columns:
                df_saida = df_saida.sort("data_vencimento")
            dataframes.append(df_saida)

    if not dataframes:
        return pl.DataFrame()

    if len(dataframes) == 1:
        return dataframes[0]

    df_resultado = pl.concat(dataframes, how="diagonal_relaxed")
    colunas_ordenacao = [
        coluna
        for coluna in ["data_referencia", "codigo_negociacao", "data_vencimento"]
        if coluna in df_resultado.columns
    ]
    if not colunas_ordenacao:
        return df_resultado

    return df_resultado.sort(*colunas_ordenacao)


def _obter_futuros_pr(datas: list[dt.date], codigo_contrato: str) -> pl.DataFrame:
    """Carrega histórico de futuros do dataset PR para uma lista de datas."""
    if not datas:
        return pl.DataFrame()

    df = _obter_pr_normalizado()
    df = _filtrar_e_renomear(df, datas, codigo_contrato)
    if df.is_empty():
        return pl.DataFrame()

    df = adicionar_vencimento(df, codigo_contrato, coluna_ticker="codigo_negociacao")
    df = _enriquecer_dados(df, codigo_contrato)
    df = _selecionar_colunas_saida(df)

    return df.sort("data_referencia", "data_vencimento")


def listar_datas_disponiveis(codigo_contrato: str) -> pl.Series:
    """Lista datas disponíveis no dataset PR para um contrato futuro."""
    return (
        _obter_pr_normalizado()
        .filter(pl.col("codigo_negociacao").str.starts_with(codigo_contrato))
        .get_column("data_referencia")
        .drop_nulls()
        .unique()
        .sort()
        .alias("data_referencia")
    )


def _buscar_price_report(
    data: dt.date, codigo: str, full_report: bool | None
) -> pl.DataFrame:
    """Busca o price report da B3, com fallback SPR→PR.

    Quando full_report é True, usa apenas o PR (completo).
    Nos demais casos (None ou False), tenta o SPR (leve) primeiro
    e faz fallback para o PR se o SPR estiver vazio.
    """
    if full_report is True:
        return _normalizar_colunas_pr(
            fetch_price_report(date=data, contract_code=codigo, full_report=True)
        )

    # SPR (leve) primeiro; PR (pesado) como fallback
    df = _normalizar_colunas_pr(
        fetch_price_report(date=data, contract_code=codigo, full_report=False)
    )
    if not df.is_empty():
        return df
    return _normalizar_colunas_pr(
        fetch_price_report(date=data, contract_code=codigo, full_report=True)
    )


def _filtrar_e_renomear(
    df: pl.DataFrame, datas: list[dt.date], codigo_contrato: str
) -> pl.DataFrame:
    return df.filter(
        pl.col("data_referencia").is_in(datas),
        pl.col("codigo_negociacao").str.starts_with(codigo_contrato),
    )


def _enriquecer_dados(df: pl.DataFrame, codigo_contrato: str) -> pl.DataFrame:
    df = df.with_columns(
        dias_uteis=bday.count_expr("data_referencia", "data_vencimento"),
        dias_corridos=(
            pl.col("data_vencimento") - pl.col("data_referencia")
        ).dt.total_days(),
    ).filter(pl.col("dias_corridos") > 0)

    eh_taxa = codigo_contrato in CONTRATOS_TAXA
    if eh_taxa:
        df = df.rename({k: v for k, v in _PRECO_PARA_TAXA.items() if k in df.columns})

    if eh_taxa:
        colunas_taxa = [c for c in df.columns if c.startswith("taxa_")]
        df = df.with_columns(pl.col(colunas_taxa).truediv(100).round(6))

    if (
        codigo_contrato == "DI1"
        and "preco_ajuste" in df.columns
        and "taxa_ajuste" in df.columns
    ):
        df = df.with_columns(
            dv01=expr_dv01("dias_uteis", "taxa_ajuste", "preco_ajuste")
        )

    if codigo_contrato in {"DI1", "DAP"} and "taxa_ajuste" in df.columns:
        df = df.with_columns(
            taxa_forward=forwards(bdays=df["dias_uteis"], rates=df["taxa_ajuste"])
        )

    return df


def _selecionar_colunas_saida(df: pl.DataFrame) -> pl.DataFrame:
    ordem_preferida = [
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
        "taxa_limite_minimo",
        "taxa_limite_maximo",
        "taxa_abertura",
        "taxa_minima",
        "taxa_maxima",
        "taxa_media",
        "taxa_fechamento",
        "taxa_ultima_oferta_compra",
        "taxa_ultima_oferta_venda",
        "taxa_ajuste",
        "taxa_forward",
    ]
    colunas_existentes = [c for c in ordem_preferida if c in df.columns]
    return df.select(colunas_existentes)
