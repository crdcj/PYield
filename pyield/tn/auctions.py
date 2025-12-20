""" """  # noqa

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
from pyield.types import DateLike, has_nullable_args

logger = logging.getLogger(__name__)

DATA_SCHEMA = {
    "quantidade_bcb": pl.Int64,
    "data_leilao": pl.String,
    "oferta": pl.Int64,
    "titulo": pl.String,
    "liquidacao": pl.String,
    "quantidade_aceita": pl.Int64,
    # "prazo": pl.Int64, algumas vezes vem sem esse campo na API
    "vencimento": pl.String,
    "benchmark": pl.String,
    "pu_medio": pl.Float64,
    "taxa_media": pl.Float64,
    "financeiro_aceito": pl.Float64,
    "pu_minimo": pl.Float64,
    "numero_edital": pl.Int64,
    "taxa_maxima": pl.Float64,
    "tipo_leilao": pl.String,
    "financeiro_bcb": pl.Int64,
    "liquidacao_segunda_volta": pl.String,
    "oferta_segunda_volta": pl.Int64,
    "financeiro_aceito_segunda_volta": pl.Float64,
    "quantidade_aceita_segunda_volta": pl.Int64,
}

COLUMN_MAP = {
    "data_leilao": "data_1v",
    "liquidacao": "data_liquidacao_1v",
    "liquidacao_segunda_volta": "data_liquidacao_2v",
    "numero_edital": "numero_edital",
    "tipo_leilao": "tipo_leilao",
    "titulo": "titulo",
    "benchmark": "benchmark",
    "vencimento": "data_vencimento",
    "oferta": "quantidade_ofertada_1v",
    "quantidade_aceita": "quantidade_aceita_1v",
    "oferta_segunda_volta": "quantidade_ofertada_2v",
    "quantidade_aceita_segunda_volta": "quantidade_aceita_2v",
    "financeiro_aceito": "financeiro_aceito_1v",
    "financeiro_aceito_segunda_volta": "financeiro_aceito_2v",
    "quantidade_bcb": "quantidade_bcb",
    "financeiro_bcb": "financeiro_bcb",
    # "prazo": "dias_corridos",
    "pu_minimo": "pu_minimo",
    "pu_medio": "pu_medio",
    "taxa_media": "taxa_media",
    "taxa_maxima": "taxa_maxima",
}

FINAL_COLUMN_ORDER = [
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
def _fetch_auction_data(auction_date: dt.date) -> list[dict]:
    """Busca os dados brutos da API do Tesouro para uma data específica."""
    endpoint = (
        "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/resultados"
    )
    params = {"dataleilao": auction_date.strftime("%d/%m/%Y")}

    response = requests.get(endpoint, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    if "registros" not in data or not data["registros"]:
        return []
    return data["registros"]


def _transform_raw_data(raw_data: list[dict]) -> pl.DataFrame:
    """Converte dados brutos em um DataFrame Polars limpo e tipado."""
    df = (
        pl.from_dicts(raw_data, schema_overrides=DATA_SCHEMA)
        .rename(COLUMN_MAP)
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
            .then(pl.col("financeiro_aceito_1v"))
            .otherwise(pl.col("quantidade_ofertada_1v") * pl.col("pu_medio")),
            financeiro_ofertado_2v=pl.when(
                pl.col("quantidade_ofertada_2v") == pl.col("quantidade_aceita_2v")
            )
            .then(pl.col("financeiro_aceito_2v"))
            .otherwise(pl.col("quantidade_ofertada_2v") * pl.col("pu_medio")),
            # Cálculo das taxas de colocação
            colocacao_1v=(
                pl.col("quantidade_aceita_1v") / pl.col("quantidade_ofertada_1v")
            ),
            colocacao_2v=(
                pl.col("quantidade_aceita_2v") / pl.col("quantidade_ofertada_2v")
            ),
            # Deixar um marcador de que o pu_medio não é original
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
            .otherwise(pl.col("pu_medio"))
            .round(6),
        )
        .with_columns(
            # Arredondamentos e transformações que criam/alteram colunas sem condicional
            cs.starts_with("financeiro_ofertado").round(2),
            (cs.starts_with("taxa") / 100).round(7),  # Percentual -> decimal
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
    dias_uteis = bday.count(df["data_liquidacao_1v"], df["data_vencimento"])
    df = df.with_columns(dias_uteis.alias("dias_uteis"))
    return df.sort("data_1v", "titulo", "data_vencimento")


def _add_duration(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calcula a duration para cada tipo de título, aplicando uma função
    linha a linha para os casos não-vetorizáveis (NTN-F e NTN-B).
    """

    def calculate_duration_per_row(row: dict) -> float:
        """Função auxiliar que aplica a lógica para uma única linha."""
        bond_type = row["titulo"]

        if bond_type == "LTN":
            return row["dias_uteis"] / 252
        elif bond_type == "NTN-F":
            # Chamada da sua função externa, linha a linha
            return duration_f(
                row["data_liquidacao_1v"], row["data_vencimento"], row["taxa_media"]
            )
        elif bond_type == "NTN-B":
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
        .map_elements(calculate_duration_per_row, return_dtype=pl.Float64)
        .alias("duration")
    )
    return df


def _add_avg_maturity(df: pl.DataFrame) -> pl.DataFrame:
    # Na metodolgia do Tesouro Nacional, a maturidade média é a mesma que a duração
    df = df.with_columns(
        pl.when(pl.col("titulo") == "LFT")
        .then(pl.col("dias_uteis") / 252)
        .otherwise(pl.col("duration"))
        .alias("prazo_medio")
    )

    return df


def _add_dv01(df: pl.DataFrame) -> pl.DataFrame:
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


def _fetch_ptax_data(auction_date: dt.date) -> pl.DataFrame:
    """Busca a PTAX para o dia útil anterior e posterior à data de referência."""
    # Voltar um dia útil com relação à data do leilão
    # Isso é importante caso seja o leilão do dia atual e não haja PTAX ainda
    min_date = bday.offset(auction_date, -1)
    # Avançar um dia útil com relação à data do leilão por conta da 2ª volta
    max_date = bday.offset(auction_date, 1)

    # Busca a série PTAX usando a função já existente
    df = bc.ptax_series(start=min_date, end=max_date)
    if df.is_empty():
        return pl.DataFrame()

    return (
        df.select("Date", "MidRate")
        .rename({"Date": "data_ref", "MidRate": "ptax"})
        .sort("data_ref")
    )


def _add_dv01_usd(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona o DV01 em USD usando um join_asof para encontrar a PTAX mais recente.
    """
    auction_date = df["data_1v"].first()
    # Busca o DataFrame da PTAX
    df_ptax = _fetch_ptax_data(auction_date=auction_date)
    if df_ptax.is_empty():
        # Se não houver dados de PTAX, retorna o DataFrame original sem alterações
        logger.warning("No PTAX data available to calculate DV01 in USD.")
        return df

    df = (
        df.sort("data_1v")  # Importante para o join_asof
        .join_asof(df_ptax, left_on="data_1v", right_on="data_ref", strategy="backward")
        .with_columns(
            (cs.starts_with("dv01") / pl.col("ptax")).round(2).name.suffix("_usd")
        )
        .drop("ptax")
    )
    return df


def auction(auction_date: DateLike) -> pl.DataFrame:
    """
    Fetches and processes Brazilian Treasury auction data for a given date.

    This function queries the Tesouro Nacional API to retrieve auction results
    for a specific date. It then processes the JSON response using the Polars
    library to create a well-structured and typed DataFrame.

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

    Args:
        auction_date: The date of the auction in the format accepted by PYield
            DateLike (e.g., "DD-MM-YYYY", datetime.date, etc.).

    Returns:
        Um DataFrame do Polars contendo os dados processados do leilão. As colunas são:
        - data_1v: Data de realização do leilão (1ª volta).
        - data_liquidacao_1v: Data de liquidação financeira da 1ª volta.
        - data_liquidacao_2v: Data de liquidação financeira da 2ª volta (se houver).
        - numero_edital: Número do edital que rege o leilão.
        - tipo_leilao: Tipo da operação (ex: "Venda", "Compra").
        - titulo: Código do título público leiloado (ex: "NTN-B", "LFT").
        - benchmark: Descrição de referência do título (ex: "NTN-B 3 anos").
        - data_vencimento: Data de vencimento do título.
        - dias_uteis: Número de dias úteis entre a liquidação (1v) e o vencimento.
        - dias_corridos: Prazo em dias corridos do título, conforme informado pela API.
        - duration: A Duração de Macaulay do título em anos, calculada entre a
            liquidação da 1ª volta e o vencimento do título.
        - prazo_medio: A maturidade média do título em anos, conforme metodologia do
            Tesouro, calculada entre a liquidação da 1ª volta e o vencimento do título.
        - quantidade_ofertada_1v: Quantidade de títulos ofertados na 1ª volta.
        - quantidade_ofertada_2v: Quantidade de títulos ofertados na 2ª volta.
        - quantidade_aceita_1v: Quantidade de títulos com propostas aceitas na 1ª volta.
        - quantidade_aceita_2v: Quantidade de títulos aceitos na 2ª volta.
        - quantidade_aceita_total: Soma das quantidades aceitas nas duas voltas.
        - financeiro_ofertado_1v: Financeiro ofertado total na 1ª volta (em BRL).
        - financeiro_ofertado_2v: Financeiro ofertado total na 2ª volta (em BRL).
        - financeiro_ofertado_total: Financeiro total ofertado nas duas voltas (em BRL).
        - financeiro_aceito_1v: Financeiro aceito total na 1ª volta (em BRL).
        - financeiro_aceito_2v: Financeiro aceito total na 2ª volta (em BRL).
        - financeiro_aceito_total: Soma do financeiro aceito nas duas voltas (em BRL).
        - quantidade_bcb: Quantidade de títulos adquirida pelo Banco Central.
        - financeiro_bcb: Financeiro adquirido pelo Banco Central.
        - colocacao_1v: Taxa de colocação da 1ª volta (quantidade aceita / ofertada).
        - colocacao_2v: Taxa de colocação da 2ª volta (quantidade aceita / ofertada).
        - colocacao_total: Taxa de colocação total (quantidade aceita / ofertada).
        - dv01_1v: DV01 da 1ª volta em BRL.
        - dv01_2v: DV01 da 2ª volta em BRL.
        - dv01_total: DV01 total do leilão em BRL.
        - dv01_1v_usd: DV01 da 1ª volta em USD usando a PTAX do dia.
        - dv01_2v_usd: DV01 da 2ª volta em USD usando a PTAX do dia.
        - dv01_total_usd: DV01 total das duas voltas em USD usando a PTAX do dia.
        - pu_minimo: Preço Unitário mínimo aceito no leilão.
        - pu_medio: Preço Unitário médio ponderado das propostas aceitas.
        - tipo_pu_medio: Indica se o PU médio é "original" (fornecido pela API) ou
            "calculado" (recalculado pela função).
        - taxa_media: Taxa de juros média das propostas aceitas (em formato decimal).
        - taxa_maxima: Taxa de juros máxima aceita no leilão (taxa de corte, em formato
            decimal).

    Retorna um DataFrame do Pandas vazio se ocorrer um erro na requisição, no
    processamento, ou se não houver dados para a data especificada.
    """
    if has_nullable_args(auction_date):
        logger.info("No auction date provided.")
        return pl.DataFrame()
    try:
        auction_date = cv.convert_dates(auction_date)
        data = _fetch_auction_data(auction_date)
        if not data:
            logger.info(f"No auction data available for {auction_date}.")
            return pl.DataFrame()
        df = _transform_raw_data(data)
        df = _add_duration(df)
        df = _add_dv01(df)
        df = _add_dv01_usd(df)
        df = _add_avg_maturity(df)
        df = df.select(FINAL_COLUMN_ORDER)

        # Substituir eventuais NaNs por None para compatibilidade com bancos de dados
        df = df.with_columns(cs.float().fill_nan(None))
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred during the API request: {e}")
        return pl.DataFrame()
    except (ValueError, TypeError) as e:
        logger.error(f"An error occurred while parsing the JSON response: {e}")
        return pl.DataFrame()
