"""
Documentação da API do BC:
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos#eyJmb3JtdWxhcmlvIjp7IiRmb3JtYXQiOiJqc29uIiwiJHRvcCI6MTAwfX0=

Exemplo de chamada:
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='2025-04-08'&@dataMovimentoFim='2025-04-08'&$top=100&$format=json
"""

import datetime as dt
import logging
from typing import Literal

import polars as pl
import polars.selectors as cs

import pyield._internal.converters as cv
from pyield import du
from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.types import DateLike
from pyield.bc._olinda import buscar_csv, montar_url, parsear_csv
from pyield.bc.sgs import ptax_serie
from pyield.tn.ntnb import duration as duration_b
from pyield.tn.ntnf import duration as duration_f

registro = logging.getLogger(__name__)

MAPA_TITULOS = {
    100000: "LTN",
    210100: "LFT",
    760199: "NTN-B",
    950199: "NTN-F",
}

ORDEM_COLUNAS_FINAL = [
    "data_leilao",
    "data_liquidacao",
    "tipo_leilao",
    "numero_edital",
    "tipo_publico",
    "titulo",
    "codigo_selic",
    "data_vencimento",
    "dias_uteis",
    "duration",
    "prazo_medio",
    "pu_medio",
    "pu_corte",
    "taxa_media",
    "taxa_corte",
    "dv01_1v",
    "dv01_2v",
    "dv01_total",
    "ptax",
    "dv01_1v_usd",
    "dv01_2v_usd",
    "dv01_total_usd",
    "quantidade_liquidada_1v",
    "quantidade_liquidada_2v",
    "quantidade_liquidada_total",
    "quantidade_ofertada_1v",
    "quantidade_ofertada_2v",
    "quantidade_ofertada_total",
    "quantidade_aceita_1v",
    "quantidade_aceita_2v",
    "quantidade_aceita_total",
    "financeiro_1v",
    "financeiro_2v",
    "financeiro_total",
]

CHAVES_ORDENACAO = ["data_leilao", "tipo_leilao", "titulo", "data_vencimento"]

URL_BASE_API = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?"


def _montar_parametros(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
) -> dict[str, str]:
    """Converte parâmetros opcionais de período em dicionário para a URL."""
    params: dict[str, str] = {}
    if inicio:
        params["dataMovimentoInicio"] = cv.converter_datas(inicio).strftime("%Y-%m-%d")
    if fim:
        params["dataMovimentoFim"] = cv.converter_datas(fim).strftime("%Y-%m-%d")
    return params


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Filtra, converte tipos e calcula colunas derivadas."""
    data_mudanca = dt.date(2024, 6, 11)

    def _duracao_por_linha(linha: dict) -> float:
        tipo = linha["titulo"]
        if tipo == "LTN":
            return linha["dias_uteis"] / 252
        if tipo == "NTN-F":
            return duration_f(
                linha["data_liquidacao"], linha["data_vencimento"], linha["taxa_media"]
            )
        if tipo == "NTN-B":
            return duration_b(
                linha["data_liquidacao"], linha["data_vencimento"], linha["taxa_media"]
            )
        return 0.0

    expr_dv01_unitario = (
        0.0001 * pl.col("pu_medio") * pl.col("duration") / (1 + pl.col("taxa_media"))
    )

    colunas_preco_taxa = ["taxa_media", "taxa_corte", "pu_medio", "pu_corte"]

    return (
        df.filter(pl.col("ofertante") == "Tesouro Nacional")
        .with_columns(
            data_leilao=pl.col("dataMovimento").str.to_date("%Y-%m-%d %H:%M:%S"),
            data_liquidacao=pl.col("dataLiquidacao").str.to_date("%Y-%m-%d %H:%M:%S"),
            data_vencimento=pl.col("dataVencimento").str.to_date("%Y-%m-%d %H:%M:%S"),
            numero_edital=pl.col("edital").cast(pl.Int64),
            tipo_publico=pl.col("tipoPublico"),
            codigo_selic=pl.col("codigoTitulo").cast(pl.Int64),
            tipo_leilao=pl.col("tipoOferta"),
            pu_medio=float_br("cotacaoMedia"),
            pu_corte=float_br("cotacaoCorte"),
            taxa_media=taxa_br("taxaMedia"),
            taxa_corte=taxa_br("taxaCorte"),
            financeiro_total=float_br("financeiro") * 1_000_000,
            quantidade_ofertada_1v=pl.col("quantidadeOfertada").cast(pl.Int64),
            quantidade_aceita_1v=pl.col("quantidadeAceita").cast(pl.Int64),
            quantidade_liquidada_1v=pl.col("quantidadeLiquidada").cast(pl.Int64),
            quantidade_ofertada_2v=pl.col("quantidadeOfertadaSegundaRodada").cast(
                pl.Int64
            ),
            quantidade_aceita_2v=pl.col("quantidadeAceitaSegundaRodada").cast(pl.Int64),
            quantidade_liquidada_2v=pl.col("quantidadeLiquidadaSegundaRodada").cast(
                pl.Int64
            ),
        )
        .with_columns(
            titulo=pl.col("codigo_selic").replace_strict(
                MAPA_TITULOS, return_dtype=pl.String
            ),
            quantidade_ofertada_total=pl.sum_horizontal(
                "quantidade_ofertada_1v", "quantidade_ofertada_2v"
            ),
            quantidade_aceita_total=pl.sum_horizontal(
                "quantidade_aceita_1v", "quantidade_aceita_2v"
            ),
            quantidade_liquidada_total=pl.sum_horizontal(
                "quantidade_liquidada_1v", "quantidade_liquidada_2v"
            ),
            dias_uteis=du.contar_expr("data_liquidacao", "data_vencimento"),
        )
        .with_columns(
            financeiro_1v=pl.when(pl.col("quantidade_aceita_1v") != 0)
            .then(
                (pl.col("quantidade_aceita_1v") / pl.col("quantidade_aceita_total"))
                * pl.col("financeiro_total")
            )
            .otherwise(0.0),
        )
        .with_columns(
            financeiro_2v=pl.col("financeiro_total") - pl.col("financeiro_1v"),
            pu_medio=pl.when(
                (pl.col("data_leilao") >= data_mudanca)
                | pl.col("titulo").is_in(["LTN", "NTN-F"])
            )
            .then("pu_medio")
            .otherwise(
                (pl.col("financeiro_1v") / pl.col("quantidade_aceita_1v")).round(6)
            ),
        )
        .with_columns(
            pl.when(pl.col("quantidade_aceita_1v") == 0)
            .then(None)
            .otherwise(pl.col(colunas_preco_taxa))
            .name.keep()
        )
        .with_columns(
            duration=pl.struct(
                "titulo",
                "data_liquidacao",
                "data_vencimento",
                "taxa_media",
                "dias_uteis",
            ).map_elements(_duracao_por_linha, return_dtype=pl.Float64)
        )
        .with_columns(
            dv01_total=expr_dv01_unitario * pl.col("quantidade_aceita_total"),
            dv01_1v=expr_dv01_unitario * pl.col("quantidade_aceita_1v"),
            dv01_2v=expr_dv01_unitario * pl.col("quantidade_aceita_2v"),
            prazo_medio=pl.when(pl.col("titulo") == "LFT")
            .then(pl.col("dias_uteis") / 252)
            .otherwise("duration"),
        )
        .with_columns(cs.float().fill_nan(None))
    )


def _buscar_ptax(df: pl.DataFrame) -> pl.DataFrame:
    """Busca a série histórica da PTAX para o intervalo de datas do DataFrame."""
    data_inicio = df["data_leilao"].min()
    data_fim = df["data_leilao"].max()
    assert isinstance(data_inicio, dt.date)
    assert isinstance(data_fim, dt.date)

    ultimo_dia_util = du.ultimo_dia_util()
    if data_inicio >= ultimo_dia_util:
        data_inicio = du.deslocar(ultimo_dia_util, -1)

    df_ptax = ptax_serie(inicio=data_inicio, fim=data_fim)
    if df_ptax.is_empty():
        return pl.DataFrame()

    return (
        df_ptax.select("data", "cotacao")
        .rename({"data": "data_ref", "cotacao": "ptax"})
        .sort("data_ref")
    )


def _adicionar_dv01_usd(df: pl.DataFrame) -> pl.DataFrame:
    """Busca PTAX e adiciona o DV01 em USD via join_asof."""
    df_ptax = _buscar_ptax(df)
    if df_ptax.is_empty():
        registro.warning("Sem dados de PTAX para calcular DV01 em USD.")
        return df

    return (
        df.sort("data_leilao")
        .join_asof(
            df_ptax, left_on="data_leilao", right_on="data_ref", strategy="backward"
        )
        .with_columns(
            (cs.starts_with("dv01") / pl.col("ptax")).round(2).name.suffix("_usd")
        )
    )


def leiloes(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    tipo_leilao: Literal["venda", "compra"] | None = None,
) -> pl.DataFrame:
    """Dados de leilões de títulos públicos federais do BCB.

    Fonte: Banco Central do Brasil. Disponível desde 12/11/2012.

    Se ambos `inicio` e `fim` forem omitidos, retorna a série
    histórica completa. Se apenas um for informado, a API do BCB
    usa o início ou fim do histórico como limite.

    Args:
        inicio: Data de início. Padrão é ``None``.
        fim: Data de fim. Padrão é ``None``.
        tipo_leilao: Tipo de leilão (``"venda"`` ou ``"compra"``).
            Padrão é ``None`` (todos).

    Returns:
        DataFrame com dados de leilões, ou DataFrame vazio
        se não houver dados.

    Output Columns:
        * data_leilao (Date): data do leilão.
        * data_liquidacao (Date): data de liquidação.
        * tipo_leilao (String): "Venda" ou "Compra".
        * numero_edital (Int64): edital normativo.
        * tipo_publico (String): categoria do comprador.
        * titulo (String): sigla do título (LTN, LFT, NTN-B, NTN-F).
        * codigo_selic (Int64): código no sistema Selic.
        * data_vencimento (Date): data de vencimento.
        * dias_uteis (Int32): dias úteis até o vencimento.
        * duration (Float64): duração de Macaulay em anos.
        * prazo_medio (Float64): prazo médio em anos.
        * pu_medio (Float64): PU médio no leilão.
        * pu_corte (Float64): PU de corte.
        * taxa_media (Float64): taxa média (decimal).
        * taxa_corte (Float64): taxa de corte (decimal).
        * dv01_1v (Float64): DV01 da 1ª volta em R$.
        * dv01_2v (Float64): DV01 da 2ª volta em R$.
        * dv01_total (Float64): DV01 total em R$.
        * ptax (Float64): PTAX (venda) para conversão USD.
        * dv01_1v_usd (Float64): DV01 da 1ª volta em USD.
        * dv01_2v_usd (Float64): DV01 da 2ª volta em USD.
        * dv01_total_usd (Float64): DV01 total em USD.
        * quantidade_liquidada_1v (Int64): qtd liquidada 1ª volta.
        * quantidade_liquidada_2v (Int64): qtd liquidada 2ª volta.
        * quantidade_liquidada_total (Int64): qtd total liquidada.
        * quantidade_ofertada_1v (Int64): qtd ofertada 1ª volta.
        * quantidade_ofertada_2v (Int64): qtd ofertada 2ª volta.
        * quantidade_ofertada_total (Int64): qtd total ofertada.
        * quantidade_aceita_1v (Int64): qtd aceita 1ª volta.
        * quantidade_aceita_2v (Int64): qtd aceita 2ª volta.
        * quantidade_aceita_total (Int64): qtd total aceita.
        * financeiro_1v (Float64): financeiro 1ª volta em R$.
        * financeiro_2v (Float64): financeiro 2ª volta em R$.
        * financeiro_total (Float64): financeiro total em R$.

    Notes:
        1v = primeira volta (rodada), 2v = segunda volta.

    Examples:
        >>> from pyield import bc
        >>> bc.leiloes(inicio="19-08-2025", fim="19-08-2025")
        shape: (5, 34)
        ┌─────────────┬─────────────────┬─────────────┬───────────────┬───┬─────────────────────────┬───────────────┬───────────────┬──────────────────┐
        │ data_leilao ┆ data_liquidacao ┆ tipo_leilao ┆ numero_edital ┆ … ┆ quantidade_aceita_total ┆ financeiro_1v ┆ financeiro_2v ┆ financeiro_total │
        │ ---         ┆ ---             ┆ ---         ┆ ---           ┆   ┆ ---                     ┆ ---           ┆ ---           ┆ ---              │
        │ date        ┆ date            ┆ str         ┆ i64           ┆   ┆ i64                     ┆ f64           ┆ f64           ┆ f64              │
        ╞═════════════╪═════════════════╪═════════════╪═══════════════╪═══╪═════════════════════════╪═══════════════╪═══════════════╪══════════════════╡
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 192           ┆ … ┆ 150000                  ┆ 2.5724e9      ┆ 0.0           ┆ 2.5724e9         │
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 192           ┆ … ┆ 751003                  ┆ 1.2804e10     ┆ 1.7124e7      ┆ 1.2822e10        │
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 193           ┆ … ┆ 300759                  ┆ 1.2899e9      ┆ 3.2635e6      ┆ 1.2932e9         │
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 194           ┆ … ┆ 500542                  ┆ 2.0717e9      ┆ 2.2457e6      ┆ 2.0739e9         │
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 194           ┆ … ┆ 500000                  ┆ 2.0107e9      ┆ 0.0           ┆ 2.0107e9         │
        └─────────────┴─────────────────┴─────────────┴───────────────┴───┴─────────────────────────┴───────────────┴───────────────┴──────────────────┘
    """
    url = montar_url(URL_BASE_API, _montar_parametros(inicio, fim))
    dados = buscar_csv(url)
    df = parsear_csv(dados)
    if df.is_empty():
        return pl.DataFrame()
    df = _processar_df(df)
    df = _adicionar_dv01_usd(df)
    df = df.select(ORDEM_COLUNAS_FINAL).sort(CHAVES_ORDENACAO)
    if tipo_leilao:
        df = df.filter(pl.col("tipo_leilao").str.to_lowercase() == tipo_leilao.lower())
    return df
