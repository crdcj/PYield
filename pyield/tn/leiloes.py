import datetime as dt
import logging
from collections.abc import Sequence

import polars as pl
import requests
from polars import selectors as cs

from pyield import du
from pyield._internal import converters as cv
from pyield._internal.br_numbers import pct_para_decimal
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty, is_collection
from pyield.bc.sgs import ptax_serie
from pyield.tn.ntnb import duration as duration_b
from pyield.tn.ntnf import duration as duration_f

logger = logging.getLogger(__name__)

# Definição unificada das colunas: (chave_api, novo_nome, tipo)
# "prazo" foi omitido pois algumas vezes não vem na API
DEFINICOES_COLUNAS = [
    ("data_leilao", "data_1v", pl.String),
    ("liquidacao", "data_liquidacao_1v", pl.String),
    ("liquidacao_segunda_volta", "data_liquidacao_2v", pl.String),
    ("numero_edital", "numero_edital", pl.Int64),
    ("tipo_leilao", "tipo_leilao", pl.String),
    ("titulo", "titulo", pl.String),
    ("benchmark", "benchmark", pl.String),
    ("vencimento", "data_vencimento", pl.String),
    ("oferta", "quantidade_ofertada_1v", pl.Int64),
    ("quantidade_aceita", "quantidade_aceita_1v", pl.Int64),
    ("oferta_segunda_volta", "quantidade_ofertada_2v", pl.Int64),
    ("quantidade_aceita_segunda_volta", "quantidade_aceita_2v", pl.Int64),
    ("financeiro_aceito", "financeiro_aceito_1v", pl.Float64),
    ("financeiro_aceito_segunda_volta", "financeiro_aceito_2v", pl.Float64),
    ("quantidade_bcb", "quantidade_bcb", pl.Int64),
    ("financeiro_bcb", "financeiro_bcb", pl.Int64),
    ("pu_minimo", "pu_minimo", pl.Float64),
    ("pu_medio", "pu_medio", pl.Float64),
    ("taxa_media", "taxa_media", pl.Float64),
    ("taxa_maxima", "taxa_maxima", pl.Float64),
]

ESQUEMA_DADOS = {api: tipo for api, _, tipo in DEFINICOES_COLUNAS}
MAPA_COLUNAS = {api: novo for api, novo, _ in DEFINICOES_COLUNAS}

ORDEM_FINAL_COLUNAS = [
    "data_1v",
    "data_liquidacao_1v",
    "data_liquidacao_2v",
    "numero_edital",
    "tipo_leilao",
    "titulo",
    "benchmark",
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
    "financeiro_ofertado_1v",
    "financeiro_ofertado_2v",
    "financeiro_ofertado_total",
    "financeiro_aceito_1v",
    "financeiro_aceito_2v",
    "financeiro_aceito_total",
    "quantidade_bcb",
    "financeiro_bcb",
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
    "tipo_pu_medio",
    "taxa_media",
    "taxa_maxima",
]


@ttl_cache()
@retry_padrao
def _buscar_dados_leilao(data_leilao: dt.date) -> list[dict]:
    """Busca os dados brutos da API do Tesouro para uma data específica.

    Exemplo de resposta da API de leilões do Tesouro:
    https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/resultados?dataleilao=30/09/2025

        {
        "registros": [
            {...},
            {
            "quantidade_bcb": 0,
            "liquidacao_segunda_volta": "30/09/2025",
            "oferta_segunda_volta": 37499,
            "data_leilao": "30/09/2025",
            "oferta": 150000,
            "titulo": "LFT",
            "liquidacao": "01/10/2025",
            "financeiro_aceito_segunda_volta": 0,
            "quantidade_aceita": 150000,
            "prazo": 1067,
            "vencimento": "01/09/2028",
            "benchmark": "LFT 3 anos",
            "pu_medio": 17434.81182753125,
            "taxa_media": 0.0669,
            "financeiro_aceito": 2615194916.22,
            "pu_minimo": 17434.632775,
            "numero_edital": 230,
            "taxa_maxima": 0.0669,
            "tipo_leilao": "Venda",
            "financeiro_bcb": 0,
            "quantidade_aceita_segunda_volta": 0
            },
            {...},
        ],
        "status": "ok"
        }
    """
    endpoint_api = (
        "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/resultados"
    )
    parametros = {"dataleilao": data_leilao.strftime("%d/%m/%Y")}

    resposta = requests.get(endpoint_api, params=parametros, timeout=10)
    resposta.raise_for_status()
    dados = resposta.json()
    if "registros" not in dados or not dados["registros"]:
        return []
    return dados["registros"]


def _transformar_dados_brutos(dados_brutos: list[dict]) -> pl.DataFrame:
    """Converte dados brutos em um DataFrame Polars limpo e tipado."""
    df = pl.from_dicts(dados_brutos, schema_overrides=ESQUEMA_DADOS)

    cols_opcionais = {
        "liquidacao_segunda_volta": pl.String,
        "oferta_segunda_volta": pl.Int64,
        "financeiro_aceito_segunda_volta": pl.Float64,
        "quantidade_liquidada": pl.Int64,
        "quantidade_aceita_segunda_volta": pl.Int64,
    }

    for nome_coluna, tipo in cols_opcionais.items():
        if nome_coluna not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=tipo).alias(nome_coluna))

    df = (
        df.rename(MAPA_COLUNAS)
        .with_columns(
            cs.starts_with("data_").str.strptime(pl.Date, "%d/%m/%Y"),
            quantidade_ofertada_total=(
                pl.sum_horizontal("quantidade_ofertada_1v", "quantidade_ofertada_2v")
            ),
            quantidade_aceita_total=(
                pl.sum_horizontal("quantidade_aceita_1v", "quantidade_aceita_2v")
            ),
            financeiro_aceito_total=(
                pl.sum_horizontal("financeiro_aceito_1v", "financeiro_aceito_2v")
            ),
            financeiro_ofertado_1v=pl.when(
                pl.col("quantidade_ofertada_1v") == pl.col("quantidade_aceita_1v")
            )
            .then("financeiro_aceito_1v")
            .otherwise(pl.col("quantidade_ofertada_1v") * pl.col("pu_medio")),
            financeiro_ofertado_2v=pl.when(
                pl.col("quantidade_ofertada_2v") == pl.col("quantidade_aceita_2v")
            )
            .then("financeiro_aceito_2v")
            .otherwise(pl.col("quantidade_ofertada_2v") * pl.col("pu_medio")),
            colocacao_1v=(
                pl.col("quantidade_aceita_1v") / pl.col("quantidade_ofertada_1v")
            ),
            colocacao_2v=(
                pl.col("quantidade_aceita_2v") / pl.col("quantidade_ofertada_2v")
            ),
            tipo_pu_medio=pl.when(pl.col("pu_medio") == 0)
            .then(pl.lit("calculado"))
            .otherwise(pl.lit("original")),
        )
        .with_columns(
            financeiro_ofertado_total=(
                pl.sum_horizontal("financeiro_ofertado_1v", "financeiro_ofertado_2v")
            ),
            colocacao_total=(
                pl.col("quantidade_aceita_total") / pl.col("quantidade_ofertada_total")
            ),
            dias_corridos=(
                pl.col("data_vencimento") - pl.col("data_liquidacao_1v")
            ).dt.total_days(),
            pu_medio=pl.when(pl.col("pu_medio") == 0)
            .then((pl.col("financeiro_aceito_1v") / pl.col("quantidade_aceita_1v")))
            .otherwise("pu_medio")
            .round(6),
        )
        .with_columns(
            cs.starts_with("financeiro_ofertado").round(2),
            pct_para_decimal(cs.starts_with("taxa")),
        )
    )
    ajustar_cols = [
        "pu_minimo",
        "pu_medio",
        "tipo_pu_medio",
        "taxa_media",
        "taxa_maxima",
    ]
    df = df.with_columns(
        pl.when(pl.col("quantidade_aceita_1v") == 0)
        .then(None)
        .otherwise(pl.col(ajustar_cols))
        .name.keep()
    )

    df = df.with_columns(
        dias_uteis=du.contar_expr("data_liquidacao_1v", "data_vencimento")
    )
    return df.sort("data_1v", "titulo", "data_vencimento")


def _adicionar_duration(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula a duration por tipo de título."""

    def calcular_duration_por_linha(row: dict) -> float:
        titulo = row["titulo"]

        if titulo == "LTN":
            return row["dias_uteis"] / 252
        if titulo == "NTN-F":
            return duration_f(
                row["data_liquidacao_1v"], row["data_vencimento"], row["taxa_media"]
            )
        if titulo == "NTN-B":
            return duration_b(
                row["data_liquidacao_1v"], row["data_vencimento"], row["taxa_media"]
            )
        return 0.0

    return df.with_columns(
        pl.struct(
            "titulo",
            "data_liquidacao_1v",
            "data_vencimento",
            "taxa_media",
            "dias_uteis",
        )
        .map_elements(calcular_duration_por_linha, return_dtype=pl.Float64)
        .alias("duration")
    )


def _adicionar_prazo_medio(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("titulo") == "LFT")
        .then(pl.col("dias_uteis") / 252)
        .otherwise("duration")
        .alias("prazo_medio")
    )


def _adicionar_dv01(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula o DV01 com base na duration da 1a volta e nas quantidades aceitas."""
    dv01_unit_expr = (
        0.0001 * pl.col("pu_medio") * pl.col("duration") / (1 + pl.col("taxa_media"))
    )

    return df.with_columns(
        dv01_1v=dv01_unit_expr * pl.col("quantidade_aceita_1v"),
        dv01_2v=dv01_unit_expr * pl.col("quantidade_aceita_2v"),
        dv01_total=dv01_unit_expr * pl.col("quantidade_aceita_total"),
    ).with_columns(cs.starts_with("dv01").round(2))


def _buscar_ptax(data_leilao: dt.date) -> pl.DataFrame:
    """Busca a PTAX para o dia util anterior e posterior a data de referencia."""
    data_min = du.deslocar(data_leilao, -1)
    data_max = du.deslocar(data_leilao, 1)

    df = ptax_serie(inicio=data_min, fim=data_max)
    if df.is_empty():
        return pl.DataFrame()

    return (
        df.select("data", "cotacao")
        .rename({"data": "data_ref", "cotacao": "ptax"})
        .sort("data_ref")
    )


def _adicionar_dv01_usd(df: pl.DataFrame) -> pl.DataFrame:
    """Adiciona o DV01 em USD usando a PTAX mais recente por join_asof."""
    data_leilao = df["data_1v"].item(0)
    df_ptax = _buscar_ptax(data_leilao=data_leilao)
    if df_ptax.is_empty():
        logger.warning("Sem dados de PTAX para calcular DV01 em USD.")
        return df

    return (
        df.sort("data_1v")
        .join_asof(df_ptax, left_on="data_1v", right_on="data_ref", strategy="backward")
        .with_columns(
            (cs.starts_with("dv01") / pl.col("ptax")).round(2).name.suffix("_usd")
        )
    )


def _selecionar_e_ordenar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Seleciona as colunas finais e ordena o DataFrame para saida."""
    colunas_selecionadas = [col for col in ORDEM_FINAL_COLUNAS if col in df.columns]
    return df.select(colunas_selecionadas).sort("data_1v", "titulo", "data_vencimento")


def leilao(data: DateLike | Sequence[DateLike]) -> pl.DataFrame:
    """Busca resultados de leilões de TPFs.

    Fonte: Tesouro Nacional. Retorna dados de quantidades, financeiros,
    taxas de colocação, duration e DV01 dos leilões nas datas informadas.

    Args:
        data: Data ou sequência de datas do leilão.

    Returns:
        DataFrame Polars com os dados processados do leilão. Se ``data`` for
        uma sequência, concatena os resultados das datas informadas. Retorna
        DataFrame vazio se não houver dados para as datas.

    Output Columns:
        * data_1v (Date): data de realização do leilão.
        * data_liquidacao_1v (Date): data de liquidação financeira da 1ª volta.
        * data_liquidacao_2v (Date): data de liquidação financeira da 2ª volta.
        * numero_edital (Int64): número do edital do leilão.
        * tipo_leilao (String): tipo da operação.
        * titulo (String): código do título público leiloado.
        * benchmark (String): descrição de referência do título.
        * data_vencimento (Date): data de vencimento do título.
        * dias_uteis (Int32): dias úteis entre liquidação e vencimento.
        * dias_corridos (Int32): dias corridos entre liquidação e vencimento.
        * duration (Float64): duration de Macaulay em anos.
        * prazo_medio (Float64): maturidade média em anos.
        * quantidade_ofertada_1v (Int64): quantidade ofertada na 1ª volta.
        * quantidade_ofertada_2v (Int64): quantidade ofertada na 2ª volta.
        * quantidade_aceita_1v (Int64): quantidade aceita na 1ª volta.
        * quantidade_aceita_2v (Int64): quantidade aceita na 2ª volta.
        * quantidade_aceita_total (Int64): quantidade aceita total.
        * financeiro_ofertado_1v (Float64): financeiro ofertado na 1ª volta.
        * financeiro_ofertado_2v (Float64): financeiro ofertado na 2ª volta.
        * financeiro_ofertado_total (Float64): financeiro ofertado total.
        * financeiro_aceito_1v (Float64): financeiro aceito na 1ª volta.
        * financeiro_aceito_2v (Float64): financeiro aceito na 2ª volta.
        * financeiro_aceito_total (Float64): financeiro aceito total.
        * quantidade_bcb (Int64): quantidade adquirida pelo Banco Central.
        * financeiro_bcb (Int64): financeiro adquirido pelo Banco Central.
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
        * pu_minimo (Float64): preço unitário mínimo aceito.
        * pu_medio (Float64): preço unitário médio ponderado aceito.
        * tipo_pu_medio (String): origem do PU médio.
        * taxa_media (Float64): taxa média aceita.
        * taxa_maxima (Float64): taxa máxima aceita.
    """
    if any_is_empty(data):
        return pl.DataFrame()

    datas: Sequence[DateLike] = (
        data if is_collection(data) else [data]  # type: ignore[assignment]
    )
    resultados = [_processar_data_unica(data) for data in datas]
    resultados = [df for df in resultados if not df.is_empty()]
    if not resultados:
        return pl.DataFrame()
    return pl.concat(resultados)


def _processar_data_unica(data_leilao: DateLike) -> pl.DataFrame:
    """Busca e processa o leilao de uma unica data."""
    data = cv.converter_datas(data_leilao)
    dados_leilao = _buscar_dados_leilao(data)
    if not dados_leilao:
        return pl.DataFrame()
    df = _transformar_dados_brutos(dados_leilao)
    df = _adicionar_duration(df)
    df = _adicionar_dv01(df)
    df = _adicionar_dv01_usd(df)
    df = _adicionar_prazo_medio(df)
    df = df.with_columns(cs.float().fill_nan(None))
    return _selecionar_e_ordenar_colunas(df)
