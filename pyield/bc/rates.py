"""Funções para buscar indicadores financeiros da API do Banco Central do Brasil.

Notas de implementação:
    - Valores são obtidos em formato percentual e convertidos para decimal
      (divididos por 100).
    - Cada tipo de taxa é arredondado para manter a mesma precisão fornecida pelo
      Banco Central:
        - SELIC Over e SELIC Meta: 4 casas decimais
        - DI Over: 8 casas decimais para taxas diárias. Para taxas anualizadas,
          o valor é arredondado para 4 casas decimais.
    - Para requisições que abrangem mais de 10 anos, o intervalo de datas é
      automaticamente dividido usando funcionalidades nativas do Polars.
"""

import datetime as dt
import logging
from enum import Enum

import polars as pl
import requests

from pyield import clock
from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty

registro = logging.getLogger(__name__)


def _extrair_valor(df: pl.DataFrame) -> float:
    """Extrai o valor escalar de um DataFrame ou retorna nan se vazio."""
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)


URL_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
CASAS_DECIMAIS_ANUALIZADA = 4  # 2 casas no formato percentual
CASAS_DECIMAIS_DIARIA = 8  # 6 casas no formato percentual

# Limite de segurança em dias, correspondendo a ~9.5 anos.
# Evita a complexidade do cálculo exato de 10 anos-calendário.
LIMITE_DIAS_SEGURO = 3500  # aprox 365 * 9.5


class SerieBC(Enum):
    """Enum para as séries disponíveis no Banco Central."""

    SELIC_OVER = 1178
    SELIC_TARGET = 432
    DI_OVER = 11


@ttl_cache()
@retry_padrao
def _chamar_api(url_api: str) -> list[dict[str, str]]:
    """Executa uma chamada GET na API do BCB e retorna o JSON.

    A API retorna um json (lista de dicts com chaves 'data' e 'valor', ambas strings):
        [{"data": "29/01/2025", "valor": "12.15"}, ...]
    """
    resposta = requests.get(url_api, timeout=10)
    resposta.raise_for_status()
    return resposta.json()


def _montar_url_download(
    serie: SerieBC, inicio: dt.date, fim: dt.date | None = None
) -> str:
    inicio_str = inicio.strftime("%d/%m/%Y")
    url = f"{URL_BASE}{serie.value}/dados?formato=json&dataInicial={inicio_str}"
    if fim:
        url += f"&dataFinal={fim.strftime('%d/%m/%Y')}"
    return url


def _buscar_requisicao(
    serie: SerieBC,
    inicio: dt.date,
    fim: dt.date | None,
) -> pl.DataFrame:
    """Busca dados da API para um intervalo."""
    esquema_esperado = {"Date": pl.Date, "Value": pl.Float64}
    url_api = _montar_url_download(serie, inicio, fim)

    try:
        dados = _chamar_api(url_api)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:  # noqa
            return pl.DataFrame(schema=esquema_esperado)
        raise

    if not dados:
        return pl.DataFrame(schema=esquema_esperado)

    return pl.from_dicts(dados).select(
        Date=pl.col("data").str.to_date("%d/%m/%Y"),
        Value=pl.col("valor").cast(pl.Float64) / 100,
    )


def _buscar_dados_url(
    serie: SerieBC, inicio: DateLike, fim: DateLike | None = None
) -> pl.DataFrame:
    """Orquestra a busca, dividindo intervalos > 10 anos em blocos."""
    data_inicio = converter_datas(inicio)
    data_fim = converter_datas(fim) if fim else clock.today()

    if (data_fim - data_inicio).days < LIMITE_DIAS_SEGURO:
        return _buscar_requisicao(serie, data_inicio, data_fim)

    inicios = pl.date_range(
        start=data_inicio, end=data_fim, interval="10y", eager=True
    )
    fins = inicios.dt.offset_by("10y").clip(upper_bound=data_fim)

    todos_dfs = [
        _buscar_requisicao(serie, ini, fim)
        for ini, fim in zip(inicios, fins)
    ]

    todos_dfs = [df for df in todos_dfs if not df.is_empty()]

    if not todos_dfs:
        return pl.DataFrame()

    return pl.concat(todos_dfs).unique(subset=["Date"], keep="first").sort("Date")


def selic_over_series(
    start: DateLike,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """Taxa SELIC Over (série SGS 1178).

    Taxa de juros média diária praticada no mercado interbancário,
    com títulos públicos como garantia.

    Args:
        start: Data inicial.
        end: Data final. Se ``None``, usa a data mais recente.

    Returns:
        DataFrame com colunas Date e Value, ou DataFrame vazio.

    Examples:
        >>> from pyield import bc
        >>> # Sem dados em 26-01-2025 (domingo). Selic mudou por reunião do Copom.
        >>> bc.selic_over_series("26-01-2025").head(5)  # Primeiras 5 linhas
        shape: (5, 2)
        ┌────────────┬────────┐
        │ Date       ┆ Value  │
        │ ---        ┆ ---    │
        │ date       ┆ f64    │
        ╞════════════╪════════╡
        │ 2025-01-27 ┆ 0.1215 │
        │ 2025-01-28 ┆ 0.1215 │
        │ 2025-01-29 ┆ 0.1215 │
        │ 2025-01-30 ┆ 0.1315 │
        │ 2025-01-31 ┆ 0.1315 │
        └────────────┴────────┘

        >>> # Buscando dados para um intervalo específico
        >>> bc.selic_over_series("14-09-2025", "17-09-2025")
        shape: (3, 2)
        ┌────────────┬───────┐
        │ Date       ┆ Value │
        │ ---        ┆ ---   │
        │ date       ┆ f64   │
        ╞════════════╪═══════╡
        │ 2025-09-15 ┆ 0.149 │
        │ 2025-09-16 ┆ 0.149 │
        │ 2025-09-17 ┆ 0.149 │
        └────────────┴───────┘
    """
    if any_is_empty(start):  # start deve ser fornecido
        return pl.DataFrame()
    df = _buscar_dados_url(SerieBC.SELIC_OVER, start, end)
    return df.with_columns(pl.col("Value").round(CASAS_DECIMAIS_ANUALIZADA))


def selic_over(date: DateLike) -> float:
    """Taxa SELIC Over para uma data específica.

    Args:
        date: Data de referência.

    Returns:
        Taxa SELIC Over ou ``nan`` se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_over("31-05-2024")
        0.104
    """
    if any_is_empty(date):
        return float("nan")
    return _extrair_valor(selic_over_series(date, date))


def selic_target_series(
    start: DateLike,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """Taxa SELIC Meta (série SGS 432).

    Taxa de juros oficial definida pelo COPOM.

    Args:
        start: Data inicial.
        end: Data final. Se ``None``, usa a data mais recente.

    Returns:
        DataFrame com colunas Date e Value, ou DataFrame vazio.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_target_series("31-05-2024", "31-05-2024")
        shape: (1, 2)
        ┌────────────┬───────┐
        │ Date       ┆ Value │
        │ ---        ┆ ---   │
        │ date       ┆ f64   │
        ╞════════════╪═══════╡
        │ 2024-05-31 ┆ 0.105 │
        └────────────┴───────┘
    """
    if any_is_empty(start):
        return pl.DataFrame()
    df = _buscar_dados_url(SerieBC.SELIC_TARGET, start, end)
    return df.with_columns(pl.col("Value").round(CASAS_DECIMAIS_ANUALIZADA))


def selic_target(date: DateLike) -> float:
    """Taxa SELIC Meta para uma data específica.

    Args:
        date: Data de referência.

    Returns:
        Taxa SELIC Meta ou ``nan`` se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_target("31-05-2024")
        0.105
    """
    if any_is_empty(date):
        return float("nan")
    return _extrair_valor(selic_target_series(date, date))


def di_over_series(
    start: DateLike,
    end: DateLike | None = None,
    annualized: bool = True,
) -> pl.DataFrame:
    """Taxa DI Over (série SGS 11).

    Taxa de juros média dos empréstimos interbancários.

    Args:
        start: Data inicial.
        end: Data final. Se ``None``, usa a data mais recente.
        annualized: Se ``True``, retorna a taxa anualizada (base
            252 d.u.). Caso contrário, retorna a taxa diária.

    Returns:
        DataFrame com colunas Date e Value, ou DataFrame vazio.

    Examples:
        >>> from pyield import bc
        >>> # Retorna todos os dados desde 29-01-2025
        >>> bc.di_over_series("29-01-2025").head(5)  # Primeiras 5 linhas
        shape: (5, 2)
        ┌────────────┬────────┐
        │ Date       ┆ Value  │
        │ ---        ┆ ---    │
        │ date       ┆ f64    │
        ╞════════════╪════════╡
        │ 2025-01-29 ┆ 0.1215 │
        │ 2025-01-30 ┆ 0.1315 │
        │ 2025-01-31 ┆ 0.1315 │
        │ 2025-02-03 ┆ 0.1315 │
        │ 2025-02-04 ┆ 0.1315 │
        └────────────┴────────┘
    """
    if any_is_empty(start):
        return pl.DataFrame()
    df = _buscar_dados_url(SerieBC.DI_OVER, start, end)
    if annualized:
        return df.with_columns(
            Value=(((pl.col("Value") + 1).pow(252)) - 1).round(
                CASAS_DECIMAIS_ANUALIZADA
            )
        )
    return df.with_columns(Value=pl.col("Value").round(CASAS_DECIMAIS_DIARIA))


def di_over(date: DateLike, annualized: bool = True) -> float:
    """Taxa DI Over para uma data específica.

    Args:
        date: Data de referência.
        annualized: Se ``True``, retorna a taxa anualizada (base
            252 d.u.). Caso contrário, retorna a taxa diária.

    Returns:
        Taxa DI Over ou ``nan`` se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.di_over("31-05-2024")
        0.104

        >>> bc.di_over("28-01-2025", annualized=False)
        0.00045513
    """
    if any_is_empty(date):
        return float("nan")
    return _extrair_valor(di_over_series(date, date, annualized))
