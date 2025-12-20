"""Módulo para consulta dos leilões de operações compromissadas (repos) realizados pelo BCB.

Fonte oficial (API OData):
https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloes_compromissadas

Exemplo de chamada bruta (CSV):
https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloes_compromissadas(dataLancamentoInicio=@dataLancamentoInicio,dataLancamentoFim=@dataLancamentoFim,horaInicio=@horaInicio,dataLiquidacao=@dataLiquidacao,dataRetorno=@dataRetorno,publicoPermitidoLeilao=@publicoPermitidoLeilao,nomeTipoOferta=@nomeTipoOferta)?@dataLancamentoInicio='2025-08-21'&@dataLancamentoFim='2025-08-21'&$format=text/csv

Exemplo (CSV original):
id                              , dataMovimento, horaInicio, publicoPermitidoLeilao, numeroComunicado, nomeTipoOferta    , ofertante    , prazoDiasCorridos, dataLiquidacao, dataRetorno, volumeAceito, taxaCorte, percentualCorte
ac1b013d13d6fb1d9d9e251b800010ee, 2025-08-21   , 09:00     , SomenteDealer         , null            , Tomador           , Banco Central,                 1, 2025-08-21    , 2025-08-22 ,    647707406, "14,9"   , 0
ac1b013d13d6fb1d9d9e251b8000121e, 2025-08-21   , 12:00     , TodoMercado           , 43716           , Compromissada 1047, Banco Central,                91, 2025-08-22    , 2025-11-21 ,      5000000, "99,78"  , "64,13"
"""  # noqa: E501

import io
import logging

import polars as pl
import requests

import pyield.converters as cv
from pyield import bday
from pyield.retry import default_retry
from pyield.types import DateLike

logger = logging.getLogger(__name__)

API_SCHEMA = {
    # Esquema explícito para leitura consistente via Polars.
    "id": pl.String,
    "dataMovimento": pl.Date,
    "horaInicio": pl.Time,
    "publicoPermitidoLeilao": pl.String,
    "numeroComunicado": pl.Int64,
    "nomeTipoOferta": pl.String,
    "ofertante": pl.String,
    "prazoDiasCorridos": pl.Int64,
    "dataLiquidacao": pl.Date,
    "dataRetorno": pl.Date,
    "volumeAceito": pl.Int64,
    "taxaCorte": pl.Float64,
    "percentualCorte": pl.Float64,
}

# Mapeamento das colunas originais da API para nomes padronizados em português.
COLUMN_MAPPING = {
    "id": "id",  # Identificador único do leilão (UUID)
    "dataMovimento": "data_leilao",
    "horaInicio": "hora_inicio",
    "publicoPermitidoLeilao": "publico_permitido",  # ['SomenteDealer', 'TodoMercado']
    "numeroComunicado": "numero_comunicado",
    "nomeTipoOferta": "tipo_oferta",
    "ofertante": "ofertante",  # Only Banco Central offers repos
    "prazoDiasCorridos": "prazo_dias_corridos",
    "dataLiquidacao": "data_liquidacao",
    "dataRetorno": "data_retorno",
    "volumeAceito": "volume_aceito",
    "taxaCorte": "taxa_corte",
    "percentualCorte": "percentual_corte",
}

API_BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloes_compromissadas(dataLancamentoInicio=@dataLancamentoInicio,dataLancamentoFim=@dataLancamentoFim,horaInicio=@horaInicio,dataLiquidacao=@dataLiquidacao,dataRetorno=@dataRetorno,publicoPermitidoLeilao=@publicoPermitidoLeilao,nomeTipoOferta=@nomeTipoOferta)?"


def _build_url(start: DateLike | None, end: DateLike | None) -> str:
    """Monta URL de consulta conforme parâmetros opcionais de início e fim.

    Regras da API:
        - Apenas start: retorna de start até o fim da série.
        - Apenas end: retorna do início da série até end.
        - Ambos ausentes: retorna a série completa.
    """
    url = API_BASE_URL
    if start:
        start = cv.convert_dates(start)
        start_str = start.strftime("%Y-%m-%d")
        url += f"@dataLancamentoInicio='{start_str}'"

    if end:
        end = cv.convert_dates(end)
        end_str = end.strftime("%Y-%m-%d")
        url += f"&@dataLancamentoFim='{end_str}'"

    url += "&$format=text/csv"  # Adiciona o formato CSV ao final

    return url


@default_retry
def _fetch_api_csv(url: str) -> str:
    """Executa requisição HTTP e retorna o corpo CSV como string.

    Decorado com ``default_retry`` para resiliência a falhas transitórias.
    Levanta exceções de status HTTP para tratamento a montante.
    """
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text


def _read_csv_data(csv_text: str) -> pl.DataFrame:
    """Lê o CSV (texto) em um DataFrame Polars com esquema definido.

    Usa decimal_comma=True para tratar números no formato brasileiro ("14,9").
    """
    return pl.read_csv(
        io.StringIO(csv_text),
        decimal_comma=True,
        null_values=["null", ""],
        schema_overrides=API_SCHEMA,
    )


def _process_df(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações numéricas e calcula prazo em dias úteis.

    Transformações:
        - volume_aceito: * 1000 (API em milhares de R$) ⇒ inteiro em R$.
        - taxa_corte: porcentagem → fração decimal (rounded 6 casas).
        - prazo_dias_uteis: calculado via calendário de negócios (bday.count).
    """
    df = df.rename(COLUMN_MAPPING).with_columns(
        volume_aceito=1000 * pl.col("volume_aceito"),
        # porcentagem -> fração
        taxa_corte=(pl.col("taxa_corte") / 100).round(6),
        # converte percentual rejeitado original em percentual aceito
        percentual_aceito=100 - pl.col("percentual_corte"),
    )

    prazos = bday.count(start=df["data_liquidacao"], end=df["data_retorno"])
    df = df.with_columns(prazos.alias("prazo_dias_uteis"))
    return df


def _handle_zero_volume(df: pl.DataFrame) -> pl.DataFrame:
    """Ajusta a taxa_corte e o percentual_aceito quando volume_aceito = 0."""
    return df.with_columns(
        taxa_corte=pl.when(pl.col("volume_aceito") == 0)
        .then(None)
        .otherwise(pl.col("taxa_corte")),
        percentual_aceito=pl.when(pl.col("volume_aceito") == 0)
        .then(0)
        .otherwise(pl.col("percentual_aceito")),
    )


def _sort_and_select_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Reordena colunas e ordena linhas para saída consistente e determinística."""
    column_sequence = [
        # "id", # ID não é relevante para o usuário final
        "data_leilao",
        "data_liquidacao",
        "data_retorno",
        "hora_inicio",
        "prazo_dias_corridos",
        "prazo_dias_uteis",
        "numero_comunicado",
        "tipo_oferta",
        "publico_permitido",
        # "ofertante", # Sempre "Banco Central", não é relevante para o usuário final
        "volume_aceito",
        "taxa_corte",
        "percentual_aceito",
    ]
    sorting_keys = ["data_leilao", "hora_inicio", "tipo_oferta"]
    return df.select(column_sequence).sort(by=sorting_keys)


def repos(
    start: DateLike | None = None,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """Consulta e retorna leilões de operações compromissadas (repos) do BCB.

    Semântica dos parâmetros de período (API OData):
        - start somente: dados de start até o fim da série.
        - end somente: dados do início da série até end.
        - ambos omitidos: série histórica completa.

    Args:
        start: Data inicial (inclusive) ou None.
        end: Data final (inclusive) ou None.

    Returns:
        DataFrame com colunas normalizadas em português e tipos
        enriquecidos (frações decimais, inteiros, datas). Em caso de erro
        retorna DataFrame vazio e registra log da exceção.

    DataFrame columns:
        - data_leilao: data de ocorrência do leilão.
        - data_liquidacao: data de liquidação (início da operação).
        - data_retorno: data de recompra / término da operação.
        - hora_inicio: horário de início do leilão.
        - prazo_dias_corridos: dias corridos até a data de retorno.
        - prazo_dias_uteis: dias úteis entre liquidação e retorno (bday.count).
        - numero_comunicado: número do comunicado/aviso do BC (pode ser nulo).
        - tipo_oferta: classif. do tipo de oferta (ex: Tomador, Compromissada 1047).
        - publico_permitido: escopo de participantes (SomenteDealer, TodoMercado).
        - volume_aceito: volume aceito no leilão em reais (convertido de milhares).
        - taxa_corte: taxa de corte (ex. 0.1490 = 14,90%). Nula se volume_aceito = 0.
        - percentual_aceito: percentual do volume ofertado efetivamente aceito (0-100).
          100 = nenhuma rejeição. 0 indica nada aceito (volume_aceito = 0).

    Notes:
        - Dados ordenados por: data_leilao, hora_inicio, tipo_oferta.

    Examples:
        >>> from pyield import bc
        >>> bc.repos(start="21-08-2025", end="21-08-2025")
        shape: (2, 12)
        ┌─────────────┬─────────────────┬──────────────┬─────────────┬───┬───────────────────┬───────────────┬────────────┬───────────────────┐
        │ data_leilao ┆ data_liquidacao ┆ data_retorno ┆ hora_inicio ┆ … ┆ publico_permitido ┆ volume_aceito ┆ taxa_corte ┆ percentual_aceito │
        │ ---         ┆ ---             ┆ ---          ┆ ---         ┆   ┆ ---               ┆ ---           ┆ ---        ┆ ---               │
        │ date        ┆ date            ┆ date         ┆ time        ┆   ┆ str               ┆ i64           ┆ f64        ┆ f64               │
        ╞═════════════╪═════════════════╪══════════════╪═════════════╪═══╪═══════════════════╪═══════════════╪════════════╪═══════════════════╡
        │ 2025-08-21  ┆ 2025-08-21      ┆ 2025-08-22   ┆ 09:00:00    ┆ … ┆ SomenteDealer     ┆ 647707406000  ┆ 0.149      ┆ 100.0             │
        │ 2025-08-21  ┆ 2025-08-22      ┆ 2025-11-21   ┆ 12:00:00    ┆ … ┆ TodoMercado       ┆ 5000000000    ┆ 0.9978     ┆ 35.87             │
        └─────────────┴─────────────────┴──────────────┴─────────────┴───┴───────────────────┴───────────────┴────────────┴───────────────────┘
    """  # noqa: E501
    try:
        url = _build_url(start=start, end=end)
        logger.debug(f"Consultando API do BC: {url}")
        api_csv = _fetch_api_csv(url)
        df = _read_csv_data(api_csv)
        if df.is_empty():
            logger.warning("Sem dados de leilões para o período especificado.")
            return pl.DataFrame()
        df = _process_df(df)
        df = _handle_zero_volume(df)
        df = _sort_and_select_columns(df)
        return df
    except Exception as e:
        logger.exception(f"Erro ao buscar dados de leilões na API do BC: {e}")
        return pl.DataFrame()
