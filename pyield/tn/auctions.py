import datetime as dt
import logging

import polars as pl
import requests
from polars import selectors as cs

from pyield import bc, bday
from pyield import converters as cv
from pyield.retry import default_retry
from pyield.tn.ntnb import duration as duration_b
from pyield.tn.ntnf import duration as duration_f
from pyield.types import DateLike, any_is_empty

logger = logging.getLogger(__name__)

# Definição unificada das colunas: chave_api -> (novo_nome, tipo)
# "prazo" foi omitido pois algumas vezes não vem na API
DEFINICOES_COLUNAS = {
    "data_leilao": ("data_1v", pl.String),
    "liquidacao": ("data_liquidacao_1v", pl.String),
    "liquidacao_segunda_volta": ("data_liquidacao_2v", pl.String),
    "numero_edital": ("numero_edital", pl.Int64),
    "tipo_leilao": ("tipo_leilao", pl.String),
    "titulo": ("titulo", pl.String),
    "benchmark": ("benchmark", pl.String),
    "vencimento": ("data_vencimento", pl.String),
    "oferta": ("quantidade_ofertada_1v", pl.Int64),
    "quantidade_aceita": ("quantidade_aceita_1v", pl.Int64),
    "oferta_segunda_volta": ("quantidade_ofertada_2v", pl.Int64),
    "quantidade_aceita_segunda_volta": ("quantidade_aceita_2v", pl.Int64),
    "financeiro_aceito": ("financeiro_aceito_1v", pl.Float64),
    "financeiro_aceito_segunda_volta": ("financeiro_aceito_2v", pl.Float64),
    "quantidade_bcb": ("quantidade_bcb", pl.Int64),
    "financeiro_bcb": ("financeiro_bcb", pl.Int64),
    "pu_minimo": ("pu_minimo", pl.Float64),
    "pu_medio": ("pu_medio", pl.Float64),
    "taxa_media": ("taxa_media", pl.Float64),
    "taxa_maxima": ("taxa_maxima", pl.Float64),
}

ESQUEMA_DADOS = {k: v[1] for k, v in DEFINICOES_COLUNAS.items()}
MAPA_COLUNAS = {k: v[0] for k, v in DEFINICOES_COLUNAS.items()}

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


@default_retry
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
    # 1. Criação inicial do DataFrame
    # O schema_overrides ajuda nos tipos, mas não cria colunas que não vieram no JSON
    df = pl.from_dicts(dados_brutos, schema_overrides=ESQUEMA_DADOS)

    # 2. Tratamento defensivo para colunas de Segunda Volta (que podem não existir)
    # Lista de colunas opcionais e seus tipos
    cols_opcionais = {
        "liquidacao_segunda_volta": pl.String,
        "oferta_segunda_volta": pl.Int64,
        "financeiro_aceito_segunda_volta": pl.Float64,
        "quantidade_liquidada": pl.Int64,
        "quantidade_aceita_segunda_volta": pl.Int64,
    }

    # Verifica se cada coluna existe; se não, cria preenchida com None (Null)
    for col_name, dtype in cols_opcionais.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col_name))

    df = (
        df.rename(MAPA_COLUNAS)
        .with_columns(
            # Conversão de datas
            cs.starts_with("data_").str.strptime(pl.Date, "%d/%m/%Y"),
            # Cálculos de totais
            quantidade_ofertada_total=(
                pl.sum_horizontal("quantidade_ofertada_1v", "quantidade_ofertada_2v")
            ),
            quantidade_aceita_total=(
                pl.sum_horizontal("quantidade_aceita_1v", "quantidade_aceita_2v")
            ),
            financeiro_aceito_total=(
                pl.sum_horizontal("financeiro_aceito_1v", "financeiro_aceito_2v")
            ),
            # Cálculo do financeiro ofertado
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
            # Cálculo das taxas de colocação
            colocacao_1v=(
                pl.col("quantidade_aceita_1v") / pl.col("quantidade_ofertada_1v")
            ),
            colocacao_2v=(
                pl.col("quantidade_aceita_2v") / pl.col("quantidade_ofertada_2v")
            ),
            # Define se o pu_medio é original ou recalculado
            tipo_pu_medio=pl.when(pl.col("pu_medio") == 0)
            .then(pl.lit("calculado"))
            .otherwise(pl.lit("original")),
        )
        .with_columns(
            # Cálculo do financeiro ofertado
            financeiro_ofertado_total=(
                pl.sum_horizontal("financeiro_ofertado_1v", "financeiro_ofertado_2v")
            ),
            colocacao_total=(
                pl.col("quantidade_aceita_total") / pl.col("quantidade_ofertada_total")
            ),
            # Algumas vezes o prazo não vem na API, então calculamos
            dias_corridos=(
                pl.col("data_vencimento") - pl.col("data_liquidacao_1v")
            ).dt.total_days(),
            # Algumas vezes o PU médio vem zero da API, então recalculamos
            pu_medio=pl.when(pl.col("pu_medio") == 0)
            .then((pl.col("financeiro_aceito_1v") / pl.col("quantidade_aceita_1v")))
            .otherwise("pu_medio")
            .round(6),
        )
        .with_columns(
            # Arredondamentos e transformações que criam/alteram colunas sem condicional
            cs.starts_with("financeiro_ofertado").round(2),
            cs.starts_with("taxa").truediv(100).round(7),  # Percentual -> decimal
        )
    )
    ajustar_cols = [
        "pu_minimo",
        "pu_medio",
        "tipo_pu_medio",
        "taxa_media",
        "taxa_maxima",
    ]
    # Se a quantidade aceita for zero, ajustar colunas específicas para None
    df = df.with_columns(
        pl.when(pl.col("quantidade_aceita_1v") == 0)
        .then(None)
        .otherwise(pl.col(ajustar_cols))
        .name.keep()
    )

    # Cálculo de dias úteis (requer acesso a colunas já convertidas)
    df = df.with_columns(
        dias_uteis=bday.count_expr("data_liquidacao_1v", "data_vencimento")
    )
    return df.sort("data_1v", "titulo", "data_vencimento")


def _adicionar_duration(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calcula a duration para cada tipo de título, aplicando uma função
    linha a linha para os casos não-vetorizáveis (NTN-F e NTN-B).
    """

    def calcular_duration_por_linha(row: dict) -> float:
        """Função auxiliar que aplica a lógica para uma única linha."""
        titulo = row["titulo"]

        if titulo == "LTN":
            return row["dias_uteis"] / 252
        elif titulo == "NTN-F":
            # Chamada da sua função externa, linha a linha
            return duration_f(
                row["data_liquidacao_1v"], row["data_vencimento"], row["taxa_media"]
            )
        elif titulo == "NTN-B":
            # Chamada da sua função externa, linha a linha
            return duration_b(
                row["data_liquidacao_1v"], row["data_vencimento"], row["taxa_media"]
            )
        else:  # LFT e outros casos
            return 0.0

    df = df.with_columns(
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
    return df


def _adicionar_prazo_medio(df: pl.DataFrame) -> pl.DataFrame:
    # Na metodologia do Tesouro Nacional, a maturidade média é a mesma que a duração
    df = df.with_columns(
        pl.when(pl.col("titulo") == "LFT")
        .then(pl.col("dias_uteis") / 252)
        .otherwise("duration")
        .alias("prazo_medio")
    )

    return df


def _adicionar_dv01(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula o DV01 com base na duration da 1ª volta e nas quantidades aceitas."""
    # 1. Define a expressão base para o cálculo do DV01 unitário.
    dv01_unit_expr = (
        0.0001 * pl.col("pu_medio") * pl.col("duration") / (1 + pl.col("taxa_media"))
    )

    df = df.with_columns(
        # 2. Criar as colunas DV01 multiplicando a expressão base pelas quantidades.
        dv01_1v=dv01_unit_expr * pl.col("quantidade_aceita_1v"),
        dv01_2v=dv01_unit_expr * pl.col("quantidade_aceita_2v"),
        dv01_total=dv01_unit_expr * pl.col("quantidade_aceita_total"),
    ).with_columns(cs.starts_with("dv01").round(2))

    return df


def _buscar_ptax(data_leilao: dt.date) -> pl.DataFrame:
    """Busca a PTAX para o dia útil anterior e posterior à data de referência."""
    # Voltar um dia útil com relação à data do leilão
    # Isso é importante caso seja o leilão do dia atual e não haja PTAX ainda
    data_min = bday.offset(data_leilao, -1)
    # Avançar um dia útil com relação à data do leilão por conta da 2ª volta
    data_max = bday.offset(data_leilao, 1)

    # Busca a série PTAX usando a função já existente
    df = bc.ptax_series(start=data_min, end=data_max)
    if df.is_empty():
        return pl.DataFrame()

    return (
        df.select("Date", "MidRate")
        .rename({"Date": "data_ref", "MidRate": "ptax"})
        .sort("data_ref")
    )


def _adicionar_dv01_usd(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona o DV01 em USD usando um join_asof para encontrar a PTAX mais recente.
    """
    data_leilao = df["data_1v"].item(0)
    # Busca o DataFrame da PTAX
    df_ptax = _buscar_ptax(data_leilao=data_leilao)
    if df_ptax.is_empty():
        # Se não houver dados de PTAX, retorna o DataFrame original sem alterações
        logger.warning("Sem dados de PTAX para calcular DV01 em USD.")
        return df

    df = (
        df.sort("data_1v")  # Importante para o join_asof
        .join_asof(df_ptax, left_on="data_1v", right_on="data_ref", strategy="backward")
        .with_columns(
            (cs.starts_with("dv01") / pl.col("ptax")).round(2).name.suffix("_usd")
        )
    )
    return df


def _selecionar_e_ordenar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Seleciona as colunas finais e ordena o DataFrame para saída."""
    colunas_selecionadas = [col for col in ORDEM_FINAL_COLUNAS if col in df.columns]
    return df.select(colunas_selecionadas).sort("data_1v", "titulo", "data_vencimento")


def auction(auction_date: DateLike) -> pl.DataFrame:
    """Consulta e processa os resultados de leilões de títulos do Tesouro Nacional.

    Busca os dados da API do Tesouro para a data informada e retorna um DataFrame
    estruturado com quantidades, financeiros, taxas de colocação, duration e DV01.

    Args:
        auction_date: Data do leilão em qualquer formato aceito por DateLike
            (ex: "DD-MM-YYYY", datetime.date).

    Returns:
        DataFrame com os dados processados do leilão. Em caso de erro na
        requisição, no processamento ou se não houver dados para a data
        especificada, retorna DataFrame vazio.

    Output Columns:
        * data_1v (Date): data de realização do leilão (1ª volta).
        * data_liquidacao_1v (Date): data de liquidação financeira da 1ª volta.
        * data_liquidacao_2v (Date): data de liquidação financeira da 2ª volta
            (se houver).
        * numero_edital (Int64): número do edital que rege o leilão.
        * tipo_leilao (String): tipo da operação (ex: "Venda", "Compra").
        * titulo (String): código do título público leiloado (ex: "NTN-B", "LFT").
        * benchmark (String): descrição de referência do título (ex: "NTN-B 3 anos").
        * data_vencimento (Date): data de vencimento do título.
        * dias_uteis (Int32): dias úteis entre a liquidação (1v) e o vencimento.
        * dias_corridos (Int32): prazo em dias corridos entre liquidação e vencimento.
        * duration (Float64): Duração de Macaulay em anos, calculada entre a liquidação
            da 1ª volta e o vencimento.
        * prazo_medio (Float64): maturidade média em anos, conforme metodologia do
            Tesouro Nacional.
        * quantidade_ofertada_1v (Int64): quantidade de títulos ofertados na 1ª volta.
        * quantidade_ofertada_2v (Int64): quantidade de títulos ofertados na 2ª volta.
        * quantidade_aceita_1v (Int64): quantidade de propostas aceitas na 1ª volta.
        * quantidade_aceita_2v (Int64): quantidade de títulos aceitos na 2ª volta.
        * quantidade_aceita_total (Int64): soma das quantidades aceitas nas duas voltas.
        * financeiro_ofertado_1v (Float64): financeiro ofertado na 1ª volta (BRL).
        * financeiro_ofertado_2v (Float64): financeiro ofertado na 2ª volta (BRL).
        * financeiro_ofertado_total (Float64): financeiro total ofertado (BRL).
        * financeiro_aceito_1v (Float64): financeiro aceito na 1ª volta (BRL).
        * financeiro_aceito_2v (Float64): financeiro aceito na 2ª volta (BRL).
        * financeiro_aceito_total (Float64): soma do financeiro aceito nas
            duas voltas (BRL).
        * quantidade_bcb (Int64): quantidade de títulos adquirida pelo Banco Central.
        * financeiro_bcb (Int64): financeiro adquirido pelo Banco Central.
        * colocacao_1v (Float64): taxa de colocação da 1ª volta (aceita / ofertada).
        * colocacao_2v (Float64): taxa de colocação da 2ª volta (aceita / ofertada).
        * colocacao_total (Float64): taxa de colocação total (aceita / ofertada).
        * dv01_1v (Float64): DV01 da 1ª volta em BRL.
        * dv01_2v (Float64): DV01 da 2ª volta em BRL.
        * dv01_total (Float64): DV01 total do leilão em BRL.
        * ptax (Float64): taxa PTAX (venda) utilizada na conversão do DV01 para USD.
        * dv01_1v_usd (Float64): DV01 da 1ª volta em USD (PTAX do dia).
        * dv01_2v_usd (Float64): DV01 da 2ª volta em USD (PTAX do dia).
        * dv01_total_usd (Float64): DV01 total em USD (PTAX do dia).
        * pu_minimo (Float64): preço unitário mínimo aceito no leilão.
        * pu_medio (Float64): preço unitário médio ponderado das propostas aceitas.
        * tipo_pu_medio (String): indica se o PU médio é "original" (da API) ou
            "calculado" (recalculado pela função).
        * taxa_media (Float64): taxa de juros média aceita (em formato decimal).
        * taxa_maxima (Float64): taxa de juros máxima aceita, taxa de corte (decimal).
    """
    if any_is_empty(auction_date):
        logger.info("Nenhuma data de leilão informada.")
        return pl.DataFrame()
    try:
        auction_date = cv.convert_dates(auction_date)
        dados_leilao = _buscar_dados_leilao(auction_date)
        if not dados_leilao:
            logger.info("Sem dados de leilão disponíveis para %s.", auction_date)
            return pl.DataFrame()
        df = _transformar_dados_brutos(dados_leilao)
        df = _adicionar_duration(df)
        df = _adicionar_dv01(df)
        df = _adicionar_dv01_usd(df)
        df = _adicionar_prazo_medio(df)
        # Substituir eventuais NaNs por None para compatibilidade com bancos de dados
        df = df.with_columns(cs.float().fill_nan(None))
        df = _selecionar_e_ordenar_colunas(df)

        return df

    except requests.exceptions.RequestException as e:
        logger.error("Erro durante a requisição da API: %s", e)
        return pl.DataFrame()
    except (ValueError, TypeError) as e:
        logger.error("Erro ao processar a resposta JSON: %s", e)
        return pl.DataFrame()
