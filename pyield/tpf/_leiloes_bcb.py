"""Leilões de TPFs publicados pelo BCB (fonte alternativa ao Tesouro Nacional).

Documentação da API:
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos
"""

import datetime as dt

import polars as pl

from pyield import du
from pyield._internal import converters as cv
from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.types import DateLike, DatesLike, any_is_empty
from pyield.bc._olinda import buscar_csv, montar_url, parsear_csv
from pyield.tpf.leiloes import _enriquecer, _validar_periodo

URL_BASE_API = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?"  # noqa: E501

MAPA_TITULOS = {
    100000: "LTN",
    210100: "LFT",
    760199: "NTN-B",
    950199: "NTN-F",
}

# Até esta data, o BCB publica cotação (% do VNA), e não PU, para LFT e NTN-B.
DATA_INICIO_PU = dt.date(2024, 6, 11)

ORDEM_FINAL_COLUNAS = [
    "data_1v",
    "data_liquidacao_1v",
    "numero_edital",
    "tipo_leilao",
    "tipo_publico",
    "titulo",
    "codigo_selic",
    "data_vencimento",
    "dias_uteis",
    "dias_corridos",
    "duration",
    "prazo_medio",
    "quantidade_ofertada_1v",
    "quantidade_ofertada_2v",
    "quantidade_aceita_1v",
    "quantidade_aceita_2v",
    "quantidade_aceita_total",
    "quantidade_liquidada_1v",
    "quantidade_liquidada_2v",
    "financeiro_ofertado_1v",
    "financeiro_ofertado_2v",
    "financeiro_ofertado_total",
    "financeiro_aceito_1v",
    "financeiro_aceito_2v",
    "financeiro_aceito_total",
    "colocacao_1v",
    "colocacao_2v",
    "colocacao_total",
    "dv01_1v",
    "dv01_2v",
    "dv01_total",
    "ptax",
    "dv01_1v_usd",
    "dv01_2v_usd",
    "dv01_total_usd",
    "pu_minimo",
    "pu_medio",
    "taxa_media",
    "taxa_maxima",
]


def _buscar_csv(inicio: dt.date | None, fim: dt.date | None) -> bytes:
    parametros = {
        "dataMovimentoInicio": inicio.isoformat() if inicio else "",
        "dataMovimentoFim": fim.isoformat() if fim else "",
    }
    return buscar_csv(montar_url(URL_BASE_API, parametros))


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Converte o CSV bruto do BCB para os nomes e unidades de ``leiloes``."""
    cotacao_sem_pu = (pl.col("data_1v") < DATA_INICIO_PU) & pl.col("titulo").is_in(
        ["LFT", "NTN-B"]
    )
    sem_aceite = pl.col("quantidade_aceita_1v") == 0

    return (
        df.filter(pl.col("ofertante") == "Tesouro Nacional")
        .select(
            data_1v=pl.col("dataMovimento").str.to_date("%Y-%m-%d %H:%M:%S"),
            data_liquidacao_1v=pl.col("dataLiquidacao").str.to_date(
                "%Y-%m-%d %H:%M:%S"
            ),
            numero_edital=pl.col("edital").cast(pl.Int64),
            tipo_leilao=pl.col("tipoOferta"),
            tipo_publico=pl.col("tipoPublico"),
            titulo=pl.col("codigoTitulo")
            .cast(pl.Int64)
            .replace_strict(MAPA_TITULOS, return_dtype=pl.String),
            codigo_selic=pl.col("codigoTitulo").cast(pl.Int64),
            data_vencimento=pl.col("dataVencimento").str.to_date("%Y-%m-%d %H:%M:%S"),
            quantidade_ofertada_1v=pl.col("quantidadeOfertada").cast(pl.Int64),
            quantidade_ofertada_2v=pl.col("quantidadeOfertadaSegundaRodada").cast(
                pl.Int64
            ),
            quantidade_aceita_1v=pl.col("quantidadeAceita").cast(pl.Int64),
            quantidade_aceita_2v=pl.col("quantidadeAceitaSegundaRodada").cast(pl.Int64),
            quantidade_liquidada_1v=pl.col("quantidadeLiquidada").cast(pl.Int64),
            quantidade_liquidada_2v=pl.col("quantidadeLiquidadaSegundaRodada").cast(
                pl.Int64
            ),
            financeiro=float_br("financeiro") * 1_000_000,
            pu_minimo=float_br("cotacaoCorte"),
            pu_medio=float_br("cotacaoMedia"),
            taxa_media=taxa_br("taxaMedia"),
            taxa_maxima=taxa_br("taxaCorte"),
        )
        .with_columns(
            quantidade_ofertada_total=pl.sum_horizontal(
                "quantidade_ofertada_1v", "quantidade_ofertada_2v"
            ),
            quantidade_aceita_total=pl.sum_horizontal(
                "quantidade_aceita_1v", "quantidade_aceita_2v"
            ),
            dias_uteis=du.contar_expr("data_liquidacao_1v", "data_vencimento"),
            dias_corridos=(
                pl.col("data_vencimento") - pl.col("data_liquidacao_1v")
            ).dt.total_days(),
        )
        .with_columns(
            # O financeiro do BCB vem em R$ milhões com uma casa decimal.
            pu_medio=pl.when(cotacao_sem_pu)
            .then(pl.col("financeiro") / pl.col("quantidade_aceita_total"))
            .otherwise("pu_medio")
            .round(6),
            pu_minimo=pl.when(cotacao_sem_pu).then(None).otherwise("pu_minimo"),
        )
        .with_columns(
            pl.when(sem_aceite)
            .then(None)
            .otherwise(pl.col("pu_minimo", "pu_medio", "taxa_media", "taxa_maxima"))
            .name.keep()
        )
        .with_columns(
            financeiro_aceito_1v=(
                pl.col("quantidade_aceita_1v") * pl.col("pu_medio")
            ).round(2),
            financeiro_aceito_2v=(
                pl.col("quantidade_aceita_2v") * pl.col("pu_medio")
            ).round(2),
            financeiro_ofertado_1v=(
                pl.col("quantidade_ofertada_1v") * pl.col("pu_medio")
            ).round(2),
            financeiro_ofertado_2v=(
                pl.col("quantidade_ofertada_2v") * pl.col("pu_medio")
            ).round(2),
            colocacao_1v=pl.col("quantidade_aceita_1v")
            / pl.col("quantidade_ofertada_1v"),
            colocacao_2v=pl.col("quantidade_aceita_2v")
            / pl.col("quantidade_ofertada_2v"),
            colocacao_total=pl.col("quantidade_aceita_total")
            / pl.col("quantidade_ofertada_total"),
        )
        .with_columns(
            financeiro_aceito_total=pl.sum_horizontal(
                "financeiro_aceito_1v", "financeiro_aceito_2v"
            ),
            financeiro_ofertado_total=pl.sum_horizontal(
                "financeiro_ofertado_1v", "financeiro_ofertado_2v"
            ),
        )
    )


def leiloes_bcb(
    *,
    data: DateLike | DatesLike | None = None,
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
) -> pl.DataFrame:
    """Busca resultados de leilões de TPFs no BCB.

    Fonte: Banco Central do Brasil, disponível desde 12/11/2012. Alternativa a
    ``yd.tpf.leiloes`` (Tesouro Nacional) para quando o Tesouro estiver
    indisponível ou ainda não trouxer a 2ª volta. Recebe os mesmos parâmetros e
    usa os mesmos nomes de colunas; colunas exclusivas de cada fonte ficam
    ausentes na outra. Os resultados podem ser unidos com
    ``pl.concat(..., how="diagonal")``.

    Args:
        data: Data ou sequência de datas do leilão. Padrão é ``None``.
        inicio: Data inicial da consulta. Padrão é ``None``.
        fim: Data final da consulta. Padrão é ``None``.

    Returns:
        DataFrame Polars com os leilões no período, ordenado por data, título e
        vencimento. Sem filtros temporais, retorna o histórico completo.
        Retorna DataFrame vazio se não houver dados.

    Output Columns:
        * data_1v (Date): data de realização do leilão.
        * data_liquidacao_1v (Date): data de liquidação financeira da 1ª volta.
        * numero_edital (Int64): número do edital do leilão.
        * tipo_leilao (String): ``"Venda"`` ou ``"Compra"``.
        * tipo_publico (String): público do leilão (exclusiva do BCB).
        * titulo (String): LTN, LFT, NTN-B ou NTN-F.
        * codigo_selic (Int64): código do título no Selic (exclusiva do BCB).
        * data_vencimento (Date): data de vencimento do título.
        * dias_uteis (Int64): dias úteis entre liquidação e vencimento.
        * dias_corridos (Int64): dias corridos entre liquidação e vencimento.
        * duration (Float64): duration de Macaulay em anos.
        * prazo_medio (Float64): maturidade média em anos.
        * quantidade_ofertada_1v (Int64): quantidade ofertada na 1ª volta.
        * quantidade_ofertada_2v (Int64): quantidade ofertada na 2ª volta.
        * quantidade_aceita_1v (Int64): quantidade aceita na 1ª volta.
        * quantidade_aceita_2v (Int64): quantidade aceita na 2ª volta.
        * quantidade_aceita_total (Int64): quantidade aceita total.
        * quantidade_liquidada_1v (Int64): quantidade liquidada na 1ª volta.
        * quantidade_liquidada_2v (Int64): quantidade liquidada na 2ª volta.
        * financeiro_ofertado_1v (Float64): quantidade ofertada na 1ª volta
            vezes ``pu_medio``.
        * financeiro_ofertado_2v (Float64): quantidade ofertada na 2ª volta
            vezes ``pu_medio``.
        * financeiro_ofertado_total (Float64): financeiro ofertado total.
        * financeiro_aceito_1v (Float64): quantidade aceita na 1ª volta vezes
            ``pu_medio``.
        * financeiro_aceito_2v (Float64): quantidade aceita na 2ª volta vezes
            ``pu_medio``.
        * financeiro_aceito_total (Float64): financeiro aceito total.
        * colocacao_1v (Float64): taxa de colocação da 1ª volta.
        * colocacao_2v (Float64): taxa de colocação da 2ª volta.
        * colocacao_total (Float64): taxa de colocação total.
        * dv01_1v (Float64): DV01 da 1ª volta em reais.
        * dv01_2v (Float64): DV01 da 2ª volta em reais.
        * dv01_total (Float64): DV01 total em reais.
        * ptax (Float64): PTAX usada na conversão para dólar.
        * dv01_1v_usd (Float64): DV01 da 1ª volta em dólar.
        * dv01_2v_usd (Float64): DV01 da 2ª volta em dólar.
        * dv01_total_usd (Float64): DV01 total em dólar.
        * pu_minimo (Float64): PU de corte.
        * pu_medio (Float64): PU médio aceito.
        * taxa_media (Float64): taxa média aceita.
        * taxa_maxima (Float64): taxa de corte.

    Notes:
        Diferenças em relação ao Tesouro Nacional:

        - O BCB não publica ``data_liquidacao_2v``, ``tipo_ocorrencia``,
          ``benchmark``, ``quantidade_bcb``, ``financeiro_bcb`` nem
          ``tipo_pu_medio``.
        - O financeiro publicado pelo BCB é arredondado a R$ 100 mil. Por
          isso, os financeiros são calculados como quantidade vezes
          ``pu_medio``, o que reproduz o Tesouro na 1ª volta; na 2ª volta, a
          diferença típica é inferior a 0,1%.
        - Na 2ª volta, a quantidade ofertada do BCB é a nominal do edital,
          enquanto o Tesouro pode informar valores ligeiramente menores.
        - Até 10/06/2024, o BCB publica cotação, e não PU, para LFT e NTN-B.
          Nesses casos, ``pu_medio`` é estimado pelo financeiro dividido pela
          quantidade aceita (sujeito ao arredondamento acima) e
          ``pu_minimo`` é nulo.

        ``data`` não pode ser combinado com ``inicio`` ou ``fim``. ``fim`` só
        pode ser usado junto com ``inicio``.

    Examples:
        >>> df = yd.tpf.leiloes_bcb(data="19-08-2025")
        >>> df.select("titulo", "quantidade_aceita_total", "pu_medio", "taxa_media")
        shape: (5, 4)
        ┌────────┬─────────────────────────┬──────────────┬────────────┐
        │ titulo ┆ quantidade_aceita_total ┆ pu_medio     ┆ taxa_media │
        │ ---    ┆ ---                     ┆ ---          ┆ ---        │
        │ str    ┆ i64                     ┆ f64          ┆ f64        │
        ╞════════╪═════════════════════════╪══════════════╪════════════╡
        │ LFT    ┆ 150000                  ┆ 17149.069465 ┆ 0.000636   │
        │ LFT    ┆ 751003                  ┆ 17072.592044 ┆ 0.001068   │
        │ NTN-B  ┆ 300759                  ┆ 4299.806542  ┆ 0.08167    │
        │ NTN-B  ┆ 500542                  ┆ 4143.249763  ┆ 0.07748    │
        │ NTN-B  ┆ 500000                  ┆ 4021.3613    ┆ 0.07323    │
        └────────┴─────────────────────────┴──────────────┴────────────┘
    """
    _validar_periodo(data, inicio, fim)

    datas = None
    if data is not None:
        if any_is_empty(data):
            return pl.DataFrame()
        convertidas = cv.converter_datas(data)
        if isinstance(convertidas, pl.Series):
            datas = sorted(convertidas.drop_nulls().to_list())
        else:
            datas = [convertidas]
        data_inicio, data_fim = datas[0], datas[-1]
    else:
        data_inicio = cv.converter_datas(inicio) if inicio is not None else None
        data_fim = cv.converter_datas(fim) if fim is not None else None

    if data_inicio is not None and data_fim is not None and data_inicio > data_fim:
        msg = "inicio deve ser menor ou igual a fim."
        raise ValueError(msg)

    df = parsear_csv(_buscar_csv(data_inicio, data_fim))
    if df.is_empty():
        return pl.DataFrame()

    df = _processar_df(df)
    if datas is not None:
        df = df.filter(pl.col("data_1v").is_in(datas))
    if df.is_empty():
        return pl.DataFrame()

    return (
        _enriquecer(df)
        .select(ORDEM_FINAL_COLUNAS)
        .sort("data_1v", "titulo", "data_vencimento")
    )
