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
from pyield.retry import retry_padrao
from pyield.types import DateLike

registro = logging.getLogger(__name__)

MAPA_COLUNAS = {
    "id": ("id", pl.String),
    "dataMovimento": ("data_leilao", pl.Date),
    "horaInicio": ("hora_inicio", pl.Time),
    "publicoPermitidoLeilao": ("publico_permitido", pl.String),
    "numeroComunicado": ("numero_comunicado", pl.Int64),
    "nomeTipoOferta": ("tipo_oferta", pl.String),
    "ofertante": ("ofertante", pl.String),
    "prazoDiasCorridos": ("prazo_dias_corridos", pl.Int64),
    "dataLiquidacao": ("data_liquidacao", pl.Date),
    "dataRetorno": ("data_retorno", pl.Date),
    "volumeAceito": ("volume_aceito", pl.Int64),
    "taxaCorte": ("taxa_corte", pl.Float64),
    "percentualCorte": ("percentual_corte", pl.Float64),
}

ESQUEMA_API = {col: dtype for col, (_, dtype) in MAPA_COLUNAS.items()}
MAPEAMENTO_COLUNAS = {col: alias for col, (alias, _) in MAPA_COLUNAS.items()}

ORDEM_COLUNAS_FINAL = [
    "data_leilao",
    "data_liquidacao",
    "data_retorno",
    "hora_inicio",
    "prazo_dias_corridos",
    "prazo_dias_uteis",
    "numero_comunicado",
    "tipo_oferta",
    "publico_permitido",
    "volume_aceito",
    "taxa_corte",
    "percentual_aceito",
]

CHAVES_ORDENACAO = ["data_leilao", "hora_inicio", "tipo_oferta"]

URL_BASE_API = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloes_compromissadas(dataLancamentoInicio=@dataLancamentoInicio,dataLancamentoFim=@dataLancamentoFim,horaInicio=@horaInicio,dataLiquidacao=@dataLiquidacao,dataRetorno=@dataRetorno,publicoPermitidoLeilao=@publicoPermitidoLeilao,nomeTipoOferta=@nomeTipoOferta)?"


def _montar_url(inicio: DateLike | None, fim: DateLike | None) -> str:
    """Monta URL de consulta conforme parâmetros opcionais de início e fim.

    Regras da API:
        - Apenas início: retorna de início até o fim da série.
        - Apenas fim: retorna do início da série até fim.
        - Ambos ausentes: retorna a série completa.
    """
    url = URL_BASE_API
    if inicio:
        inicio = cv.converter_datas(inicio)
        inicio_str = inicio.strftime("%Y-%m-%d")
        url += f"@dataLancamentoInicio='{inicio_str}'"

    if fim:
        fim = cv.converter_datas(fim)
        fim_str = fim.strftime("%Y-%m-%d")
        url += f"&@dataLancamentoFim='{fim_str}'"

    url += "&$format=text/csv"  # Adiciona o formato CSV ao final

    return url


@retry_padrao
def _buscar_csv_api(url: str) -> str:
    """Executa requisição HTTP e retorna o corpo CSV como string.

    Decorado com ``retry_padrao`` para resiliência a falhas transitórias.
    Levanta exceções de status HTTP para tratamento a montante.
    """
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.text


def _ler_csv(csv_texto: str) -> pl.DataFrame:
    """Lê o CSV (texto) em um DataFrame Polars com esquema definido.

    Usa decimal_comma=True para tratar números no formato brasileiro ("14,9").
    """
    return pl.read_csv(
        io.StringIO(csv_texto),
        decimal_comma=True,
        null_values=["null", ""],
        schema_overrides=ESQUEMA_API,
    )


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações numéricas e calcula prazo em dias úteis.

    Transformações:
        - volume_aceito: * 1000 (API em milhares de R$) ⇒ inteiro em R$.
        - taxa_corte: porcentagem → fração decimal (rounded 6 casas).
        - prazo_dias_uteis: calculado via calendário de negócios (bday.count).
    """
    df = df.rename(MAPEAMENTO_COLUNAS).with_columns(
        volume_aceito=1000 * pl.col("volume_aceito"),
        # porcentagem -> fração
        taxa_corte=pl.col("taxa_corte").truediv(100).round(6),
        # converte percentual rejeitado original em percentual aceito
        percentual_aceito=100 - pl.col("percentual_corte"),
    )

    df = df.with_columns(
        prazo_dias_uteis=bday.count_expr("data_liquidacao", "data_retorno"),
    )
    return df


def _ajustar_volume_zero(df: pl.DataFrame) -> pl.DataFrame:
    """Ajusta a taxa_corte e o percentual_aceito quando volume_aceito = 0."""
    return df.with_columns(
        taxa_corte=pl.when(pl.col("volume_aceito") == 0)
        .then(None)
        .otherwise("taxa_corte"),
        percentual_aceito=pl.when(pl.col("volume_aceito") == 0)
        .then(0)
        .otherwise("percentual_aceito"),
    )


def _ordenar_selecionar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Reordena colunas e ordena linhas para saída consistente e determinística."""
    colunas_selecionadas = [col for col in ORDEM_COLUNAS_FINAL if col in df.columns]
    return df.select(colunas_selecionadas).sort(by=CHAVES_ORDENACAO)


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

    Output Columns:
        * data_leilao (Date): data de ocorrência do leilão.
        * data_liquidacao (Date): data de liquidação (início da operação).
        * data_retorno (Date): data de recompra / término da operação.
        * hora_inicio (Time): horário de início do leilão.
        * prazo_dias_corridos (Int64): dias corridos até a data de retorno.
        * prazo_dias_uteis (Int64): dias úteis entre liquidação e retorno (bday.count).
        * numero_comunicado (Int64): número do comunicado/aviso do BC (pode ser nulo).
        * tipo_oferta (String): classif. do tipo de oferta (ex: Tomador, Compromissada 1047).
        * publico_permitido (String): escopo de participantes (SomenteDealer, TodoMercado).
        * volume_aceito (Int64): volume aceito no leilão em reais (convertido de milhares).
        * taxa_corte (Float64): taxa de corte (ex. 0.1490 = 14,90%). Nula se volume_aceito = 0.
        * percentual_aceito (Float64): percentual do volume ofertado efetivamente aceito (0-100).
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
        url = _montar_url(inicio=start, fim=end)
        registro.debug(f"Consultando API do BC: {url}")
        csv_api = _buscar_csv_api(url)
        df = _ler_csv(csv_api)
        if df.is_empty():
            registro.warning("Sem dados de leilões para o período especificado.")
            return pl.DataFrame()
        df = _processar_df(df)
        df = _ajustar_volume_zero(df)
        df = _ordenar_selecionar_colunas(df)
        return df
    except Exception as e:
        registro.exception(f"Erro ao buscar dados de leilões na API do BC: {e}")
        return pl.DataFrame()
