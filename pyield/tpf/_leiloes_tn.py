"""Leilões de TPFs publicados pelo Tesouro Nacional.

Além do ETL da fonte, reúne o enriquecimento (duration, DV01 e prazo médio)
compartilhado com a fonte BCB.
"""

import datetime as dt
import logging

import polars as pl
import requests
from polars import selectors as cs

from pyield import du
from pyield._internal import converters as cv
from pyield._internal.br_numbers import pct_para_decimal
from pyield._internal.retry import requisitar
from pyield._internal.types import DateLike, DatesLike, any_is_empty
from pyield.bc.sgs import ptax_serie
from pyield.tpf.titulos import ltn, ntnb, ntnf

logger = logging.getLogger(__name__)

# Definição unificada das colunas: (chave_api, novo_nome, tipo)
# "prazo" foi omitido pois algumas vezes não vem na API
DEFINICOES_COLUNAS = [
    ("data_leilao", "data_1v", pl.String),
    ("liquidacao", "data_liquidacao_1v", pl.String),
    ("liquidacao_segunda_volta", "data_liquidacao_2v", pl.String),
    ("numero_edital", "numero_edital", pl.Int64),
    ("tipo_leilao", "tipo_leilao", pl.String),
    ("tipo_ocorrencia", "tipo_ocorrencia", pl.String),
    ("titulo", "titulo", pl.String),
    ("benchmark", "benchmark", pl.String),
    ("vencimento", "data_vencimento", pl.String),
    ("oferta", "quantidade_ofertada_1v", pl.Int64),
    ("quantidade_aceita", "quantidade_aceita_1v", pl.Int64),
    ("oferta_segunda_volta", "quantidade_ofertada_2v", pl.Int64),
    ("quantidade_aceita_segunda_volta", "quantidade_aceita_2v", pl.Int64),
    ("quantidade_liquidada", "quantidade_liquidada_1v", pl.Int64),
    ("quantidade_liquidada_segunda_volta", "quantidade_liquidada_2v", pl.Int64),
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

# Chaves que a API pode omitir; a ausência de qualquer outra é erro da fonte.
CHAVES_OPCIONAIS = {
    "benchmark",  # descritiva; leilões extraordinários podem não trazê-la
    "liquidacao_segunda_volta",
    "oferta_segunda_volta",
    "financeiro_aceito_segunda_volta",
    "tipo_ocorrencia",
    "quantidade_liquidada",
    "quantidade_liquidada_segunda_volta",
    "quantidade_aceita_segunda_volta",
}

ORDEM_FINAL_COLUNAS = [
    "data_1v",
    "data_liquidacao_1v",
    "data_liquidacao_2v",
    "numero_edital",
    "tipo_leilao",
    "tipo_ocorrencia",
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
    "quantidade_liquidada_1v",
    "quantidade_liquidada_2v",
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


def _buscar_dados_leiloes(
    data_leilao: dt.date | None = None,
    ano_inicial: int | None = None,
) -> list[dict]:
    """Busca os dados brutos da API do Tesouro.

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
    parametros = {}
    if data_leilao is not None:
        parametros["dataleilao"] = data_leilao.strftime("%d/%m/%Y")
    if ano_inicial is not None:
        parametros["anoinicial"] = str(ano_inicial)

    resposta = requisitar(
        requests.get, endpoint_api, params=parametros, timeout_leitura=5
    )
    return resposta.json().get("registros") or []


def _processar_registros(dados_brutos: list[dict]) -> pl.DataFrame:
    """Converte dados brutos em um DataFrame Polars limpo e tipado."""
    ausentes = ESQUEMA_DADOS.keys() - CHAVES_OPCIONAIS - set().union(*dados_brutos)
    if ausentes:
        msg = f"Resposta do Tesouro sem as chaves obrigatórias: {sorted(ausentes)}."
        raise ValueError(msg)

    # Sem aceite na 1ª volta, preços e taxas não têm significado e ficam nulos.
    colunas_sem_aceite = [
        "pu_minimo",
        "pu_medio",
        "tipo_pu_medio",
        "taxa_media",
        "taxa_maxima",
    ]
    # A API publica 0 quando não informa o PU médio.
    pu_medio_zerado = pl.col("pu_medio") == 0
    return (
        # O schema completo cria como nulas as chaves opcionais ausentes e
        # descarta chaves fora da definição (ex.: "prazo").
        pl.from_dicts(dados_brutos, schema=ESQUEMA_DADOS)
        .rename(MAPA_COLUNAS)
        .with_columns(
            cs.starts_with("data_").str.to_date("%d/%m/%Y"),
            quantidade_ofertada_total=(
                pl.sum_horizontal("quantidade_ofertada_1v", "quantidade_ofertada_2v")
            ),
            quantidade_aceita_total=(
                pl.sum_horizontal("quantidade_aceita_1v", "quantidade_aceita_2v")
            ),
            financeiro_aceito_total=(
                pl.sum_horizontal("financeiro_aceito_1v", "financeiro_aceito_2v")
            ),
            tipo_pu_medio=pl.when(pu_medio_zerado)
            .then(pl.lit("calculado"))
            .otherwise(pl.lit("original")),
            pu_medio=pl.when(pu_medio_zerado)
            .then((pl.col("financeiro_aceito_1v") / pl.col("quantidade_aceita_1v")))
            .otherwise("pu_medio")
            .round(6),
            # A API publica 0 quando não informa o PU mínimo.
            pu_minimo=pl.when(pl.col("pu_minimo") == 0)
            .then(None)
            .otherwise("pu_minimo"),
        )
        .with_columns(
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
            colocacao_total=(
                pl.col("quantidade_aceita_total") / pl.col("quantidade_ofertada_total")
            ),
            dias_corridos=(
                pl.col("data_vencimento") - pl.col("data_liquidacao_1v")
            ).dt.total_days(),
        )
        .with_columns(
            financeiro_ofertado_total=(
                pl.sum_horizontal("financeiro_ofertado_1v", "financeiro_ofertado_2v")
            ),
        )
        .with_columns(
            cs.starts_with("financeiro_ofertado").round(2),
            pct_para_decimal(cs.starts_with("taxa")),
        )
        .with_columns(
            pl.when(pl.col("quantidade_aceita_1v") == 0)
            .then(None)
            .otherwise(pl.col(colunas_sem_aceite))
            .name.keep(),
            dias_uteis=du.contar_expr("data_liquidacao_1v", "data_vencimento"),
        )
    )


def _adicionar_duration(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula a duration por tipo de título."""
    return df.with_columns(
        duration=(
            pl.when(pl.col("titulo") == "LTN")
            .then(ltn.duration_expr("data_liquidacao_1v", "data_vencimento"))
            .when(pl.col("titulo") == "NTN-F")
            .then(
                ntnf.duration_expr(
                    "data_liquidacao_1v", "data_vencimento", "taxa_media"
                )
            )
            .when(pl.col("titulo") == "NTN-B")
            .then(
                ntnb.duration_expr(
                    "data_liquidacao_1v", "data_vencimento", "taxa_media"
                )
            )
            .otherwise(0.0)
        )
    )


def _adicionar_prazo_medio(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        prazo_medio=pl.when(pl.col("titulo") == "LFT")
        .then(pl.col("dias_uteis") / 252)
        .otherwise("duration")
    )


def _adicionar_dv01(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula o DV01 unitário por título e multiplica pelas quantidades aceitas."""
    dv01_unitario = (
        pl.when(pl.col("titulo") == "LTN")
        .then(ltn.dv01_expr("data_liquidacao_1v", "data_vencimento", "taxa_media"))
        .when(pl.col("titulo") == "NTN-F")
        .then(ntnf.dv01_expr("data_liquidacao_1v", "data_vencimento", "taxa_media"))
        .when(pl.col("titulo") == "NTN-B")
        .then(
            ntnb.dv01_expr(
                "data_liquidacao_1v", "data_vencimento", "taxa_media", "pu_medio"
            )
        )
        .otherwise(0.0)
    )

    return df.with_columns(
        dv01_1v=dv01_unitario * pl.col("quantidade_aceita_1v"),
        dv01_2v=dv01_unitario * pl.col("quantidade_aceita_2v"),
        dv01_total=dv01_unitario * pl.col("quantidade_aceita_total"),
    ).with_columns(cs.starts_with("dv01").round(2))


def _buscar_ptax(df: pl.DataFrame) -> pl.DataFrame:
    """Busca a PTAX do dia útil anterior ao primeiro leilão até o último leilão."""
    inicio = df["data_1v"].min()
    fim = df["data_1v"].max()
    assert isinstance(inicio, dt.date)
    assert isinstance(fim, dt.date)
    return (
        ptax_serie(inicio=du.deslocar(inicio, -1), fim=fim)
        .select(data_ref="data", ptax=pl.col("cotacao").cast(pl.Float64))
        .sort("data_ref")
    )


def _adicionar_dv01_usd(df: pl.DataFrame) -> pl.DataFrame:
    """Adiciona o DV01 em USD usando a PTAX mais recente por join_asof.

    Sem PTAX disponível, as colunas ``ptax`` e ``dv01_*_usd`` ficam nulas.
    """
    df_ptax = _buscar_ptax(df)
    if df_ptax.is_empty():
        logger.warning("Sem dados de PTAX para calcular DV01 em USD.")

    return (
        df.sort("data_1v")
        .join_asof(df_ptax, left_on="data_1v", right_on="data_ref", strategy="backward")
        .with_columns(
            (cs.starts_with("dv01") / pl.col("ptax")).round(2).name.suffix("_usd")
        )
    )


def _selecionar_e_ordenar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Seleciona as colunas finais e ordena o DataFrame para saída."""
    return df.select(ORDEM_FINAL_COLUNAS).sort("data_1v", "titulo", "data_vencimento")


def leiloes_tn(
    *,
    data: DateLike | DatesLike | None = None,
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
) -> pl.DataFrame:
    """Busca resultados de leilões de TPFs no Tesouro Nacional.

    Exposta por ``yd.tpf.leiloes(fonte="tn")``, cuja docstring documenta o
    contrato.
    """
    _validar_periodo(data, inicio, fim)

    if data is not None:
        if any_is_empty(data):
            return pl.DataFrame()

        datas = cv.converter_datas(data)
        if isinstance(datas, pl.Series):
            if datas.null_count() > 0:
                msg = "data deve conter apenas datas válidas."
                raise ValueError(msg)
            dias = datas.unique().sort().to_list()
        else:
            dias = [datas]

        # A API é consultada uma vez por dia útil; o processamento é único.
        registros = [
            registro
            for dia in dias
            if du.eh_dia_util(dia)
            for registro in _buscar_dados_leiloes(data_leilao=dia)
        ]
        return _processar_dados_leiloes(registros)

    data_inicio = cv.converter_datas(inicio) if inicio is not None else None
    data_fim = cv.converter_datas(fim) if fim is not None else None

    if data_inicio is not None and data_fim is not None and data_inicio > data_fim:
        msg = "inicio deve ser menor ou igual a fim."
        raise ValueError(msg)

    dados_leilao = _buscar_dados_leiloes(
        ano_inicial=data_inicio.year if data_inicio is not None else None
    )
    return _processar_dados_leiloes(dados_leilao, inicio=data_inicio, fim=data_fim)


def _processar_dados_leiloes(
    dados_leilao: list[dict],
    inicio: dt.date | None = None,
    fim: dt.date | None = None,
) -> pl.DataFrame:
    if not dados_leilao:
        return pl.DataFrame()

    df = _processar_registros(dados_leilao)
    if inicio is not None:
        df = df.filter(pl.col("data_1v") >= inicio)
    if fim is not None:
        df = df.filter(pl.col("data_1v") <= fim)
    if df.is_empty():
        return pl.DataFrame()
    return _selecionar_e_ordenar_colunas(_enriquecer(df))


def _validar_periodo(
    data: DateLike | DatesLike | None,
    inicio: DateLike | None,
    fim: DateLike | None,
) -> None:
    if data is not None and (inicio is not None or fim is not None):
        msg = "data não pode ser combinado com inicio ou fim."
        raise ValueError(msg)
    if fim is not None and inicio is None:
        msg = "fim só pode ser usado junto com inicio."
        raise ValueError(msg)


def _enriquecer(df: pl.DataFrame) -> pl.DataFrame:
    """Adiciona duration, DV01 e prazo médio, comuns às fontes TN e BCB."""
    df = _adicionar_duration(df)
    df = _adicionar_dv01(df)
    df = _adicionar_dv01_usd(df)
    df = _adicionar_prazo_medio(df)
    return df.with_columns(cs.float().fill_nan(None))
