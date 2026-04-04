"""
Módulo para acessar a API de cotações PTAX do Banco Central do Brasil (BCB)

Exemplo de chamada à API:
    https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='09-01-2025'&@dataFinalCotacao='09-10-2025'&$format=text/csv

Exemplo de resposta CSV da API do BCB:
cotacaoCompra, cotacaoVenda, dataHoraCotacao
2814         , 2828        , 1984-12-03 11:29:00.0
2814         , 2828        , 1984-12-03 16:38:00.0
2867         , 2881        , 1984-12-04 11:17:00.0
"0,843"      , "0,845"     , 1995-01-02 18:20:00.0
...
"5,4272"     , "5,4278"    , 2025-09-08 13:09:40.608
"5,4272"     , "5,4278"    , 2025-09-09 13:07:27.786
"5,4117"     , "5,4123"    , 2025-09-10 13:06:29.196

ATENÇÃO: a fração de segundo varia entre .0 (datas antigas) e .608
(datas recentes). Usar %.f (variável) no parsing, nunca %.3f (fixo).
"""

import datetime as dt

import polars as pl
import requests

import pyield._internal.converters as cv
from pyield import relogio
from pyield._internal.br_numbers import float_br
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike

URL_API_PTAX = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"


def _montar_url_api(inicio: dt.date, fim: dt.date) -> str:
    inicio_str = inicio.strftime("%m-%d-%Y")
    fim_str = fim.strftime("%m-%d-%Y")
    return (
        f"{URL_API_PTAX}"
        f"@dataInicial='{inicio_str}'"
        f"&@dataFinalCotacao='{fim_str}'"
        f"&$format=text/csv"
    )


@ttl_cache()
@retry_padrao
def _buscar_texto_api(url: str) -> bytes:
    resposta = requests.get(url, timeout=10)
    resposta.raise_for_status()
    return resposta.content


def _parsear_df(conteudo_csv: bytes) -> pl.DataFrame:
    """Lê o CSV bruto da API PTAX em DataFrame com todas as colunas como string."""
    return pl.read_csv(conteudo_csv, infer_schema=False)


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            cotacao_compra=float_br("cotacaoCompra"),
            cotacao_venda=float_br("cotacaoVenda"),
            data_hora=pl.col("dataHoraCotacao").str.to_datetime(
                format="%Y-%m-%d %H:%M:%S%.f", strict=False
            ),
        )
        .with_columns(
            data=pl.col("data_hora").cast(pl.Date),
            hora=pl.col("data_hora").dt.time(),
            cotacao_media=pl.mean_horizontal("cotacao_compra", "cotacao_venda").round(
                5
            ),
        )
        .sort("data_hora")
        .unique(subset=["data"], keep="last")
        .select("data", "hora", "cotacao_compra", "cotacao_venda", "cotacao_media")
        .sort("data")
    )


def ptax_series(
    start: DateLike | None = None,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """Cotações de fechamento do Dólar PTAX (taxa de câmbio).

    Fonte: Banco Central do Brasil (BCB). Frequência diária.

    Se `start` não for informado, usa 28.11.1984 (primeira data
    disponível). Se `end` não for informado, usa a data de hoje.

    Args:
        start: Data de início da consulta. Padrão é ``None``.
        end: Data de fim da consulta. Padrão é ``None``.

    Returns:
        DataFrame com as cotações do período, ou DataFrame vazio
        se não houver dados.

    Output Columns:
        * data (Date): data da cotação.
        * hora (Time): hora da cotação.
        * cotacao_compra (Float64): taxa de compra em R$.
        * cotacao_venda (Float64): taxa de venda em R$.
        * cotacao_media (Float64): média de compra/venda (5 casas).

    Notes:
        Disponível desde 28.11.1984; refere-se às taxas
        administradas até março de 1990 e às taxas livres a
        partir de então (Resolução 1690, de 18.3.1990). A
        partir de março de 1992, essa taxa recebeu a
        denominação PTAX. Desde 1 de julho de 2011 (Circular
        3506), a PTAX corresponde à média aritmética das
        taxas obtidas em quatro consultas diárias aos dealers
        de câmbio.

        Documentação da API:
        https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/documentacao

    Examples:
        >>> from pyield import bc
        >>> bc.ptax_series(start="20-04-2025", end="25-04-2025")
        shape: (4, 5)
        ┌────────────┬──────────────┬────────────────┬───────────────┬───────────────┐
        │ data       ┆ hora         ┆ cotacao_compra ┆ cotacao_venda ┆ cotacao_media │
        │ ---        ┆ ---          ┆ ---            ┆ ---           ┆ ---           │
        │ date       ┆ time         ┆ f64            ┆ f64           ┆ f64           │
        ╞════════════╪══════════════╪════════════════╪═══════════════╪═══════════════╡
        │ 2025-04-22 ┆ 13:09:35.629 ┆ 5.749          ┆ 5.7496        ┆ 5.7493        │
        │ 2025-04-23 ┆ 13:06:30.443 ┆ 5.6874         ┆ 5.688         ┆ 5.6877        │
        │ 2025-04-24 ┆ 13:04:29.639 ┆ 5.6732         ┆ 5.6738        ┆ 5.6735        │
        │ 2025-04-25 ┆ 13:09:26.592 ┆ 5.684          ┆ 5.6846        ┆ 5.6843        │
        └────────────┴──────────────┴────────────────┴───────────────┴───────────────┘

        >>> bc.ptax_series(start="02-01-1995", end="06-01-1995")
        shape: (5, 5)
        ┌────────────┬──────────┬────────────────┬───────────────┬───────────────┐
        │ data       ┆ hora     ┆ cotacao_compra ┆ cotacao_venda ┆ cotacao_media │
        │ ---        ┆ ---      ┆ ---            ┆ ---           ┆ ---           │
        │ date       ┆ time     ┆ f64            ┆ f64           ┆ f64           │
        ╞════════════╪══════════╪════════════════╪═══════════════╪═══════════════╡
        │ 1995-01-02 ┆ 18:20:00 ┆ 0.843          ┆ 0.845         ┆ 0.844         │
        │ 1995-01-03 ┆ 18:25:00 ┆ 0.844          ┆ 0.846         ┆ 0.845         │
        │ 1995-01-04 ┆ 18:12:00 ┆ 0.844          ┆ 0.846         ┆ 0.845         │
        │ 1995-01-05 ┆ 18:07:00 ┆ 0.842          ┆ 0.844         ┆ 0.843         │
        │ 1995-01-06 ┆ 18:12:00 ┆ 0.839          ┆ 0.841         ┆ 0.84          │
        └────────────┴──────────┴────────────────┴───────────────┴───────────────┘
    """
    if start:
        start = cv.converter_datas(start)
    else:
        start = dt.date(1984, 11, 28)  # Primeira data disponível na API

    if end:
        end = cv.converter_datas(end)
    else:
        end = relogio.hoje()

    url = _montar_url_api(start, end)
    texto = _buscar_texto_api(url)
    df = _parsear_df(texto)
    if df.is_empty():
        return pl.DataFrame()
    return _processar_df(df)


def ptax(date: DateLike) -> float:
    """Cotação PTAX média de fechamento para uma data específica.

    Args:
        date: Data desejada.

    Returns:
        Taxa média (cotacao_media) do dia, ou ``nan`` se não
        houver cotação (feriado, fim de semana ou data futura).

    Examples:
        >>> from pyield import bc
        >>> # Busca a PTAX para um dia útil
        >>> bc.ptax("22-08-2025")
        5.4389

        >>> # Busca a PTAX para um fim de semana (sem dados)
        >>> bc.ptax("23-08-2025")
        nan
    """
    dados_ptax = ptax_series(start=date, end=date)
    if dados_ptax.is_empty():
        return float("nan")
    return dados_ptax["cotacao_media"].item(0)
