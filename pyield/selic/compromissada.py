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

import pyield._internal.converters as cv
from pyield import du
from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.types import DateLike
from pyield.bc._olinda import buscar_csv, montar_url, parsear_csv

URL_BASE_API = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloes_compromissadas(dataLancamentoInicio=@dataLancamentoInicio,dataLancamentoFim=@dataLancamentoFim,horaInicio=@horaInicio,dataLiquidacao=@dataLiquidacao,dataRetorno=@dataRetorno,publicoPermitidoLeilao=@publicoPermitidoLeilao,nomeTipoOferta=@nomeTipoOferta)?"


def _montar_parametros(
    inicio: DateLike | None,
    fim: DateLike | None,
) -> dict[str, str]:
    """Converte parâmetros opcionais de período em dicionário para a URL."""
    params: dict[str, str] = {}
    if inicio:
        params["dataLancamentoInicio"] = cv.converter_datas(inicio).strftime("%Y-%m-%d")
    if fim:
        params["dataLancamentoFim"] = cv.converter_datas(fim).strftime("%Y-%m-%d")
    return params


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Renomeia, converte tipos e calcula colunas derivadas em um único select."""
    vol_zero = pl.col("volumeAceito").cast(pl.Int64) == 0
    return df.select(
        data_leilao=pl.col("dataMovimento").str.to_date("%Y-%m-%d"),
        data_liquidacao=pl.col("dataLiquidacao").str.to_date("%Y-%m-%d"),
        data_retorno=pl.col("dataRetorno").str.to_date("%Y-%m-%d"),
        hora_inicio=pl.col("horaInicio").str.to_time("%H:%M"),
        prazo_dc=pl.col("prazoDiasCorridos").cast(pl.Int64),
        prazo_du=du.contar_expr("dataLiquidacao", "dataRetorno"),
        comunicado=pl.col("numeroComunicado").cast(pl.Int64),
        tipo_oferta=pl.col("nomeTipoOferta"),
        publico=pl.col("publicoPermitidoLeilao"),
        financeiro_aceito=1000 * pl.col("volumeAceito").cast(pl.Float64),
        taxa_corte=pl.when(vol_zero).then(None).otherwise(taxa_br("taxaCorte")),
        pct_aceito=pl.when(vol_zero)
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
        - prazo_dc (Int64): dias corridos até a data de retorno.
        - prazo_du (Int64): dias úteis entre liquidação e retorno.
        - comunicado (Int64): número do comunicado/aviso do BC (pode ser nulo).
        - tipo_oferta (String): classif. do tipo de oferta (ex: Tomador, Compromissada 1047).
        - publico (String): público permitido no leilão (SomenteDealer, TodoMercado).
        - financeiro_aceito (Float64): financeiro aceito no leilão em reais (convertido de milhares).
        - taxa_corte (Float64): taxa de corte (ex. 0.1490 = 14,90%). Nula se financeiro_aceito = 0.
        - pct_aceito (Float64): percentual do volume ofertado efetivamente aceito (0-100).
          100 = nenhuma rejeição. 0 indica nada aceito (volume_aceito = 0).

    Notes:
        - Dados ordenados por: data_leilao, hora_inicio, tipo_oferta.

    Examples:
        >>> import polars as pl
        >>> _ = pl.Config.set_tbl_width_chars(210)
        >>> _ = pl.Config.set_tbl_cols(-1)
        >>> import pyield as yd
        >>> yd.selic.compromissadas(inicio="21-08-2025", fim="21-08-2025")
        shape: (2, 12)
        ┌─────────────┬─────────────────┬──────────────┬─────────────┬──────────┬──────────┬────────────┬────────────────────┬───────────────┬───────────────────┬────────────┬────────────┐
        │ data_leilao ┆ data_liquidacao ┆ data_retorno ┆ hora_inicio ┆ prazo_dc ┆ prazo_du ┆ comunicado ┆ tipo_oferta        ┆ publico       ┆ financeiro_aceito ┆ taxa_corte ┆ pct_aceito │
        │ ---         ┆ ---             ┆ ---          ┆ ---         ┆ ---      ┆ ---      ┆ ---        ┆ ---                ┆ ---           ┆ ---               ┆ ---        ┆ ---        │
        │ date        ┆ date            ┆ date         ┆ time        ┆ i64      ┆ i64      ┆ i64        ┆ str                ┆ str           ┆ f64               ┆ f64        ┆ f64        │
        ╞═════════════╪═════════════════╪══════════════╪═════════════╪══════════╪══════════╪════════════╪════════════════════╪═══════════════╪═══════════════════╪════════════╪════════════╡
        │ 2025-08-21  ┆ 2025-08-21      ┆ 2025-08-22   ┆ 09:00:00    ┆ 1        ┆ 1        ┆ null       ┆ Tomador            ┆ SomenteDealer ┆ 6.4771e11         ┆ 0.149      ┆ 100.0      │
        │ 2025-08-21  ┆ 2025-08-22      ┆ 2025-11-21   ┆ 12:00:00    ┆ 91       ┆ 64       ┆ 43716      ┆ Compromissada 1047 ┆ TodoMercado   ┆ 5.0000e9          ┆ 0.9978     ┆ 35.87      │
        └─────────────┴─────────────────┴──────────────┴─────────────┴──────────┴──────────┴────────────┴────────────────────┴───────────────┴───────────────────┴────────────┴────────────┘
        >>> _ = pl.Config.restore_defaults()
        >>> _ = pl.Config.set_tbl_width_chars(150)
    """
    params = _montar_parametros(inicio, fim)
    url = montar_url(URL_BASE_API, params)
    dados = buscar_csv(url)
    df = parsear_csv(dados)
    if df.is_empty():
        return pl.DataFrame()
    return _processar_df(df)
