"""
Exemplo de resposta da API de leilões do Tesouro:
https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/resultados?dataleilao=30/09/2025

{
  "registros": [
    {
      "quantidade_bcb": 0,
      "data_leilao": "30/09/2025",
      "oferta": 150000,
      "titulo": "NTN-B",
      "liquidacao": "01/10/2025",
      "quantidade_aceita": 10000,
      "prazo": 1050,
      "vencimento": "15/08/2028",
      "benchmark": "NTN-B 3 anos",
      "pu_medio": 4351.91338575,
      "taxa_media": 8.1375,
      "financeiro_aceito": 43518468.56,
      "pu_minimo": 4351.846856,
      "numero_edital": 231,
      "taxa_maxima": 8.1375,
      "tipo_leilao": "Venda",
      "financeiro_bcb": 0
    },
    {
      "quantidade_bcb": 0,
      "liquidacao_segunda_volta": "2025-10-01T00:00:00.000Z",
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
    {
      "quantidade_bcb": 0,
      "liquidacao_segunda_volta": "2025-10-01T00:00:00.000Z",
      "oferta_segunda_volta": 187499,
      "data_leilao": "30/09/2025",
      "oferta": 750000,
      "titulo": "LFT",
      "liquidacao": "01/10/2025",
      "financeiro_aceito_segunda_volta": 0,
      "quantidade_aceita": 590500,
      "prazo": 2162,
      "vencimento": "01/09/2031",
      "benchmark": "LFT 6 anos",
      "pu_medio": 17360.929790289472,
      "taxa_media": 0.1064,
      "financeiro_aceito": 10250991400.75,
      "pu_minimo": 17359.849959,
      "numero_edital": 230,
      "taxa_maxima": 0.1064,
      "tipo_leilao": "Venda",
      "financeiro_bcb": 0,
      "quantidade_aceita_segunda_volta": 0
    },
    {
      "quantidade_bcb": 0,
      "liquidacao_segunda_volta": "2025-10-02T00:00:00.000Z",
      "oferta_segunda_volta": 37500,
      "data_leilao": "30/09/2025",
      "oferta": 150000,
      "titulo": "NTN-B",
      "liquidacao": "01/10/2025",
      "financeiro_aceito_segunda_volta": 0,
      "quantidade_aceita": 150000,
      "prazo": 2511,
      "vencimento": "15/08/2032",
      "benchmark": "NTN-B 7 anos",
      "pu_medio": 1255.811481,
      "taxa_media": 7.774,
      "financeiro_aceito": 627417931.33,
      "pu_minimo": 0,
      "numero_edital": 232,
      "taxa_maxima": 7.774,
      "tipo_leilao": "Venda",
      "financeiro_bcb": 0,
      "quantidade_aceita_segunda_volta": 0
    },
    {
      "quantidade_bcb": 0,
      "data_leilao": "30/09/2025",
      "oferta": 150000,
      "titulo": "NTN-B",
      "liquidacao": "01/10/2025",
      "quantidade_aceita": 65450,
      "prazo": 7167,
      "vencimento": "15/05/2045",
      "benchmark": "NTN-B 25 anos",
      "pu_medio": 1564.3624848235295,
      "taxa_media": 7.28,
      "financeiro_aceito": 267111959.97,
      "pu_minimo": 0,
      "numero_edital": 232,
      "taxa_maxima": 7.28,
      "tipo_leilao": "Venda",
      "financeiro_bcb": 0
    }
  ],
  "status": "ok"
}
"""  # noqa

import datetime as dt
import logging

import pandas as pd
import polars as pl
import requests
from polars import selectors as cs

from pyield import bc, bday
from pyield import date_converter as dc
from pyield.date_converter import DateScalar
from pyield.tn.ntnb import duration as duration_b
from pyield.tn.ntnf import duration as duration_f

logger = logging.getLogger(__name__)

DATA_SCHEMA = {
    "quantidade_bcb": pl.Int64,
    "data_leilao": pl.String,
    "oferta": pl.Int64,
    "titulo": pl.String,
    "liquidacao": pl.String,
    "quantidade_aceita": pl.Int64,
    "prazo": pl.Int64,
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
    "financeiro_aceito": "financeiro_1v",
    "financeiro_aceito_segunda_volta": "financeiro_2v",
    "quantidade_bcb": "quantidade_bcb",
    "financeiro_bcb": "financeiro_bcb",
    "prazo": "dias_corridos",
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
    "financeiro_1v",
    "financeiro_2v",
    "financeiro_total",
    "quantidade_bcb",
    "financeiro_bcb",
    "dv01_1v",
    "dv01_2v",
    "dv01_total",
    "dv01_1v_usd",
    "dv01_2v_usd",
    "dv01_total_usd",
    "pu_minimo",
    "pu_medio",
    "taxa_media",
    "taxa_maxima",
]


def _fetch_auction_data(auction_date: dt.date) -> list[dict]:
    """Busca os dados brutos da API do Tesouro para uma data específica."""
    endpoint = (
        "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/resultados"
    )
    params = {"dataleilao": auction_date.strftime("%d/%m/%Y")}

    response = requests.get(endpoint, params=params)
    response.raise_for_status()
    data = response.json()
    if "registros" not in data or not data["registros"]:
        logger.warning(f"Nenhum dado de leilão encontrado para a data: {auction_date}")
        return []
    return data["registros"]


def _fetch_ptax_data(auction_date: dt.date) -> pl.DataFrame:
    """Busca a PTAX para o dia útil anterior e posterior à data de referência."""
    # Voltar um dia útil com relação à data do leilão
    # Isso é importante caso seja o leilão do dia atual e não haja PTAX ainda
    min_date = bday.offset(auction_date, -1)
    # Avançar um dia útil com relação à data do leilão por conta da 2ª volta
    max_date = bday.offset(auction_date, 1)

    # Busca a série PTAX usando a função já existente
    df_pd = bc.ptax_series(start=min_date, end=max_date)
    if df_pd.empty:
        return pl.DataFrame()

    # Converte para Polars, seleciona, renomeia e ordena (importante para join_asof)
    return (
        pl.from_pandas(df_pd)
        .select(["Date", "MidRate"])
        .rename({"Date": "data_ref", "MidRate": "ptax"})
        .sort("data_ref")
    )


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
            [
                "titulo",
                "data_liquidacao_1v",
                "data_vencimento",
                "taxa_media",
                "dias_uteis",
            ]
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
    """
    Calcula o DV01 para o leilão de forma 100% vetorizada em Polars.
    """
    # 1. Define a expressão base para o cálculo do DV01 unitário.
    dv01_unit_expr = (
        0.0001 * pl.col("pu_medio") * pl.col("duration") / (1 + pl.col("taxa_media"))
    )

    df = df.with_columns(
        # 2. Criar as colunas DV01 multiplicando a expressão base pelas quantidades.
        (dv01_unit_expr * pl.col("quantidade_aceita_1v")).alias("dv01_1v"),
        (dv01_unit_expr * pl.col("quantidade_aceita_2v")).alias("dv01_2v"),
        (dv01_unit_expr * pl.col("quantidade_aceita_total")).alias("dv01_total"),
    ).with_columns(cs.starts_with("dv01").round(2))

    return df


def _fetch_ptax_data(auction_date: dt.date) -> pl.DataFrame:
    """
    Busca a série histórica da PTAX no intervalo de datas especificado
    e retorna como um DataFrame Polars.
    """
    # Voltar um dia útil com relação à data do leilão
    # Isso é importante caso seja o leilão do dia atual e não haja PTAX ainda
    min_date = bday.offset(auction_date, -1)
    # Avançar um dia útil com relação à data do leilão por conta da 2ª volta
    max_date = bday.offset(auction_date, 1)

    # Busca a série PTAX usando a função já existente
    df_pd = bc.ptax_series(start=min_date, end=max_date)
    if df_pd.empty:
        return pl.DataFrame()

    # Converte para Polars, seleciona, renomeia e ordena (importante para join_asof)
    return (
        pl.from_pandas(df_pd)
        .select(["Date", "MidRate"])
        .rename({"Date": "data_ref", "MidRate": "ptax"})
        .sort("data_ref")
    )


def _add_dv01_usd(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona o DV01 em USD usando um join_asof para encontrar a PTAX mais recente.
    """
    auction_date = df.get_column("data_1v").min()
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


def _transform_raw_data(raw_data: list[dict]) -> pl.DataFrame:
    """Converte dados brutos em um DataFrame Polars limpo e tipado."""
    if not raw_data:
        return pl.DataFrame()

    df = (
        pl.from_dicts(raw_data, schema=DATA_SCHEMA)
        .rename(COLUMN_MAP)
        .with_columns(
            # Conversão de datas
            cs.starts_with("data_")
            .exclude("data_liquidacao_2v")
            .str.strptime(pl.Date, "%d/%m/%Y"),
            # Tratamento da data de 2ª volta que tem formato diferente
            pl.col("data_liquidacao_2v").str.strptime(
                pl.Date, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False
            ),
            # Cálculos de totais
            pl.sum_horizontal("quantidade_aceita_1v", "quantidade_aceita_2v").alias(
                "quantidade_aceita_total"
            ),
            pl.sum_horizontal("financeiro_1v", "financeiro_2v").alias(
                "financeiro_total"
            ),
            # Converter de percentual para formato decimal
            (cs.starts_with("taxa") / 100).round(7),
        )
    )

    # Cálculo de dias úteis (requer acesso a colunas já convertidas)
    dias_uteis = bday.count(df["data_liquidacao_1v"], df["data_vencimento"])
    df = df.with_columns(pl.Series(name="dias_uteis", values=dias_uteis))
    return df


def auction(auction_date: DateScalar) -> pd.DataFrame:
    """
    Fetches and processes Brazilian Treasury auction data for a given date.

    This function queries the Tesouro Nacional API to retrieve auction results
    for a specific date. It then processes the JSON response using the Polars
    library to create a well-structured and typed DataFrame.

    Args:
        auction_date: The date of the auction in the format accepted by PYield
            DateScalar (e.g., "DD-MM-YYYY", datetime.date, etc.).

    Returns:
        A Pandas DataFrame containing the processed auction data.
        Returns an empty DataFrame if the request fails or no data is found.
    """
    try:
        auction_date = dc.convert_input_dates(auction_date)
        data = _fetch_auction_data(auction_date)
        df = _transform_raw_data(data)
        df = _add_duration(df)
        df = _add_dv01(df)
        df = _add_dv01_usd(df)
        df = _add_avg_maturity(df)
        df = df.select(FINAL_COLUMN_ORDER)
        return df.to_pandas(use_pyarrow_extension_array=True)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")
        return pd.DataFrame()
    except ValueError as e:
        print(f"An error occurred while parsing the JSON response: {e}")
        return pd.DataFrame()
