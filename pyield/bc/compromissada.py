"""Módulo para consulta dos leilões de operações compromissadas (repos) realizados pelo BCB.

Fonte oficial (API OData):
https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloes_compromissadas

Exemplo de chamada bruta (CSV):
https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloes_compromissadas(dataLancamentoInicio=@dataLancamentoInicio,dataLancamentoFim=@dataLancamentoFim,horaInicio=@horaInicio,dataLiquidacao=@dataLiquidacao,dataRetorno=@dataRetorno,publicoPermitidoLeilao=@publicoPermitidoLeilao,nomeTipoOferta=@nomeTipoOferta)?@dataLancamentoInicio='2025-08-21'&@dataLancamentoFim='2025-08-21'&$format=text/csv

Exemplo (CSV original):
id                              , dataMovimento, horaInicio, publicoPermitidoLeilao, numeroComunicado, nomeTipoOferta    , ofertante    , prazoDiasCorridos, dataLiquidacao, dataRetorno, volumeAceito, taxaCorte, percentualCorte
ac1b013d13d6fb1d9d9e251b800010ee, 2025-08-21   , 09:00     , SomenteDealer         , null            , Tomador           , Banco Central,                 1, 2025-08-21    , 2025-08-22 ,    647707406, "14,9"   , 0
ac1b013d13d6fb1d9d9e251b8000121e, 2025-08-21   , 12:00     , TodoMercado           , 43716           , Compromissada 1047, Banco Central,                91, 2025-08-22    , 2025-11-21 ,      5000000, "99,78"  , "64,13"
"""

import polars as pl
import requests

import pyield._internal.converters as cv
from pyield import du
from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike

URL_BASE_API = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloes_compromissadas(dataLancamentoInicio=@dataLancamentoInicio,dataLancamentoFim=@dataLancamentoFim,horaInicio=@horaInicio,dataLiquidacao=@dataLiquidacao,dataRetorno=@dataRetorno,publicoPermitidoLeilao=@publicoPermitidoLeilao,nomeTipoOferta=@nomeTipoOferta)?"


def _montar_url(
    inicio: DateLike | None,
    fim: DateLike | None,
) -> str:
    """Monta URL de consulta conforme parâmetros opcionais de período.

    Regras da API:
        - Apenas inicio: retorna de inicio até o fim da série.
        - Apenas fim: retorna do início da série até fim.
        - Ambos ausentes: retorna a série completa.
    """
    url = URL_BASE_API
    if inicio:
        inicio_dt = cv.converter_datas(inicio)
        inicio_str = inicio_dt.strftime("%Y-%m-%d")
        url += f"@dataLancamentoInicio='{inicio_str}'"

    if fim:
        fim_dt = cv.converter_datas(fim)
        fim_str = fim_dt.strftime("%Y-%m-%d")
        url += f"&@dataLancamentoFim='{fim_str}'"

    url += "&$format=text/csv"  # Adiciona o formato CSV ao final

    return url


@retry_padrao
def _buscar_csv_api(url: str) -> bytes:
    """Executa requisição HTTP e retorna o corpo CSV como string.

    Decorado com ``retry_padrao`` para resiliência a falhas transitórias.
    Levanta exceções de status HTTP para tratamento a montante.
    """
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.content


def _ler_csv(csv_bytes: bytes) -> pl.DataFrame:
    """Lê o CSV (bytes) em um DataFrame Polars sem inferência de tipos."""
    return pl.read_csv(
        csv_bytes,
        infer_schema=False,
        null_values=["null", ""],
    )


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Renomeia, converte tipos e calcula colunas derivadas em um único select."""
    vol_zero = pl.col("volumeAceito").cast(pl.Int64) == 0
    return df.select(
        data_leilao=pl.col("dataMovimento").str.to_date("%Y-%m-%d"),
        data_liquidacao=pl.col("dataLiquidacao").str.to_date("%Y-%m-%d"),
        data_retorno=pl.col("dataRetorno").str.to_date("%Y-%m-%d"),
        hora_inicio=pl.col("horaInicio").str.to_time("%H:%M"),
        prazo_dias_corridos=pl.col("prazoDiasCorridos").cast(pl.Int64),
        prazo_dias_uteis=du.contar_expr("dataLiquidacao", "dataRetorno"),
        numero_comunicado=pl.col("numeroComunicado").cast(pl.Int64),
        tipo_oferta=pl.col("nomeTipoOferta"),
        publico_permitido=pl.col("publicoPermitidoLeilao"),
        volume_aceito=1000 * pl.col("volumeAceito").cast(pl.Int64),
        taxa_corte=pl.when(vol_zero).then(None).otherwise(taxa_br("taxaCorte")),
        percentual_aceito=pl.when(vol_zero)
        .then(0.0)
        .otherwise(100 - float_br("percentualCorte")),
    ).sort("data_leilao", "hora_inicio", "tipo_oferta")


def compromissadas(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
) -> pl.DataFrame:
    """Consulta e retorna leilões de operações compromissadas do BCB.

    Semântica dos parâmetros de período (API OData):
        - inicio somente: dados de inicio até o fim da série.
        - fim somente: dados do início da série até fim.
        - ambos omitidos: série histórica completa.

    Args:
        inicio: Data inicial (inclusive) ou None.
        fim: Data final (inclusive) ou None.

    Returns:
        DataFrame com colunas normalizadas em português e tipos
        enriquecidos (frações decimais, inteiros, datas).

    Output Columns:
        - data_leilao (Date): data de ocorrência do leilão.
        - data_liquidacao (Date): data de liquidação (início da operação).
        - data_retorno (Date): data de recompra / término da operação.
        - hora_inicio (Time): horário de início do leilão.
        - prazo_dias_corridos (Int64): dias corridos até a data de retorno.
                - prazo_dias_uteis (Int64): dias úteis entre liquidação e retorno
                    (du.contar).
        - numero_comunicado (Int64): número do comunicado/aviso do BC (pode ser nulo).
        - tipo_oferta (String): classif. do tipo de oferta (ex: Tomador, Compromissada 1047).
        - publico_permitido (String): escopo de participantes (SomenteDealer, TodoMercado).
        - volume_aceito (Int64): volume aceito no leilão em reais (convertido de milhares).
        - taxa_corte (Float64): taxa de corte (ex. 0.1490 = 14,90%). Nula se volume_aceito = 0.
        - percentual_aceito (Float64): percentual do volume ofertado efetivamente aceito (0-100).
          100 = nenhuma rejeição. 0 indica nada aceito (volume_aceito = 0).

    Notes:
        - Dados ordenados por: data_leilao, hora_inicio, tipo_oferta.

    Examples:
        >>> from pyield import bc
        >>> bc.compromissadas(inicio="21-08-2025", fim="21-08-2025")
        shape: (2, 12)
        ┌─────────────┬─────────────────┬──────────────┬─────────────┬───┬───────────────────┬───────────────┬────────────┬───────────────────┐
        │ data_leilao ┆ data_liquidacao ┆ data_retorno ┆ hora_inicio ┆ … ┆ publico_permitido ┆ volume_aceito ┆ taxa_corte ┆ percentual_aceito │
        │ ---         ┆ ---             ┆ ---          ┆ ---         ┆   ┆ ---               ┆ ---           ┆ ---        ┆ ---               │
        │ date        ┆ date            ┆ date         ┆ time        ┆   ┆ str               ┆ i64           ┆ f64        ┆ f64               │
        ╞═════════════╪═════════════════╪══════════════╪═════════════╪═══╪═══════════════════╪═══════════════╪════════════╪═══════════════════╡
        │ 2025-08-21  ┆ 2025-08-21      ┆ 2025-08-22   ┆ 09:00:00    ┆ … ┆ SomenteDealer     ┆ 647707406000  ┆ 0.149      ┆ 100.0             │
        │ 2025-08-21  ┆ 2025-08-22      ┆ 2025-11-21   ┆ 12:00:00    ┆ … ┆ TodoMercado       ┆ 5000000000    ┆ 0.9978     ┆ 35.87             │
        └─────────────┴─────────────────┴──────────────┴─────────────┴───┴───────────────────┴───────────────┴────────────┴───────────────────┘
    """
    url = _montar_url(inicio=inicio, fim=fim)
    csv_api = _buscar_csv_api(url)
    df = _ler_csv(csv_api)
    if df.is_empty():
        return pl.DataFrame()
    return _processar_df(df)
