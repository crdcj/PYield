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

import logging
from enum import Enum
from typing import Any

import polars as pl
import requests

from pyield import clock
from pyield.converters import convert_dates
from pyield.retry import default_retry
from pyield.types import DateLike, any_is_empty

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
DECIMAL_PLACES_ANNUALIZED = 4  # 2 decimal places in percentage format
DECIMAL_PLACES_DAILY = 8  # 6 decimal places in percentage format

# 404 Not Found error code for resource not found in the API
ERROR_CODE_NOT_FOUND = 404

# 400 Bad Request error code for invalid requests
ERROR_CODE_BAD_REQUEST = 400

# Limite de segurança em dias, correspondendo a ~9.5 anos.
# Evita a complexidade do cálculo exato de 10 anos-calendário.
SAFE_DAYS_THRESHOLD = 3500  # aprox 365 * 9.5


class BCSerie(Enum):
    """Enum para as séries disponíveis no Banco Central."""

    SELIC_OVER = 1178
    SELIC_TARGET = 432
    DI_OVER = 11


@default_retry
def _do_api_call(api_url: str) -> list[dict[str, Any]]:
    """Executa uma chamada GET na API do BCB e retorna o JSON."""
    response = requests.get(api_url, timeout=10)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


def _build_download_url(
    serie: BCSerie, start: DateLike, end: DateLike | None = None
) -> str:
    """Constrói a URL para download de dados das séries do Banco Central.

    Args:
        serie: Valor enum da série a buscar.
        start: Data inicial para os dados a buscar.
        end: Data final para os dados.

    Returns:
        URL formatada para a requisição da API.
    """
    start = convert_dates(start)
    start_str = start.strftime("%d/%m/%Y")

    api_url = BASE_URL
    api_url += f"{serie.value}/dados?formato=json"
    api_url += f"&dataInicial={start_str}"

    if end:
        end = convert_dates(end)
        end_str = end.strftime("%d/%m/%Y")
        api_url += f"&dataFinal={end_str}"

    return api_url


def _fetch_request(
    serie: BCSerie,
    start: DateLike,
    end: DateLike | None,
) -> pl.DataFrame:
    """Função worker que busca dados da API."""
    # Define o esquema esperado para o DataFrame de retorno.
    # Isso é crucial para os casos em que a API não retorna dados.
    expected_schema = {"Date": pl.Date, "Value": pl.Float64}

    api_url = _build_download_url(serie, start, end)

    try:
        data = _do_api_call(api_url)
        if not data:
            logger.warning(f"No data available for the requested period: {api_url}")
            return pl.DataFrame(schema=expected_schema)

        df = (
            pl.from_dicts(data)
            .with_columns(
                Date=pl.col("data").str.to_date("%d/%m/%Y"),
                Value=pl.col("valor").cast(pl.Float64) / 100,
            )
            .select("Date", "Value")
        )
        return df

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:  # noqa
            logger.warning(
                f"Resource not found (404), treated as no data: {e.request.url}"
            )
            return pl.DataFrame(schema=expected_schema)

        # Qualquer outro erro HTTP final para o programa.
        raise


def _fetch_data_from_url(
    serie: BCSerie, start: DateLike, end: DateLike | None = None
) -> pl.DataFrame:
    """Orquestra a busca de dados da API do Banco Central.

    Trata requisições maiores que 10 anos dividindo-as em chunks menores usando
    polars date_range.

    Args:
        serie: Enum da série a buscar.
        start: Data inicial para os dados a buscar.
        end: Data final para os dados.

    Returns:
        DataFrame com os dados requisitados.
    """
    # 1. Converter datas usando a função auxiliar existente
    start_date = convert_dates(start)
    # Se a data final não for fornecida, usar a data de hoje para o cálculo do período
    end_date = convert_dates(end) if end else clock.today()

    # Verificação simples e pragmática baseada em dias. Se o período for
    # menor que nosso limite de segurança, faz uma chamada única.
    if (end_date - start_date).days < SAFE_DAYS_THRESHOLD:
        return _fetch_request(serie, start_date, end_date)

    # 3. Se for maior, quebrar em pedaços (chunking)
    logger.info("Date range exceeds 10 years. Fetching data in chunks.")

    duration_str = "10y"

    chunk_starts = pl.date_range(
        start=start_date, end=end_date, interval=duration_str, eager=True
    )

    chunk_ends = chunk_starts.dt.offset_by(duration_str)

    chunks_df = pl.DataFrame({"start": chunk_starts, "end": chunk_ends}).with_columns(
        pl.when(pl.col("end") > end_date).then(end_date).otherwise("end").alias("end")
    )

    all_dfs = [
        _fetch_request(serie, chunk["start"], chunk["end"])
        for chunk in chunks_df.iter_rows(named=True)
    ]

    all_dfs = [df for df in all_dfs if not df.is_empty()]

    if not all_dfs:
        return pl.DataFrame()

    return pl.concat(all_dfs).unique(subset=["Date"], keep="first").sort("Date")


def selic_over_series(
    start: DateLike,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """Busca a taxa SELIC Over do Banco Central do Brasil.

    A taxa SELIC Over é a taxa de juros média diária efetivamente praticada
    entre bancos no mercado interbancário, usando títulos públicos como garantia.

    Exemplo de URL da API:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: Data inicial para buscar os dados. Se None, retorna dados desde
            a data mais antiga disponível.
        end: Data final para buscar os dados. Se None, retorna dados até a
            data mais recente disponível.

    Returns:
        DataFrame contendo colunas Date e Value com a taxa SELIC Over, ou
        DataFrame vazio se dados não estiverem disponíveis.

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
    if any_is_empty(start):  # Start must be provided
        return pl.DataFrame()
    df = _fetch_data_from_url(BCSerie.SELIC_OVER, start, end)
    return df.with_columns(pl.col("Value").round(DECIMAL_PLACES_ANNUALIZED))


def selic_over(date: DateLike) -> float:
    """Busca o valor da taxa SELIC Over para uma data específica.

    Função de conveniência que retorna apenas o valor (não o DataFrame) para a
    data especificada.

    Args:
        date: Data de referência para buscar a taxa SELIC Over.

    Returns:
        Taxa SELIC Over como float ou NaN se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_over("31-05-2024")
        0.104
    """
    if any_is_empty(date):
        return float("nan")
    df = selic_over_series(date, date)
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)


def selic_target_series(
    start: DateLike,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """Busca a taxa SELIC Meta do Banco Central do Brasil.

    A taxa SELIC Meta é a taxa de juros oficial definida pelo Comitê de Política
    Monetária (COPOM) do Banco Central do Brasil.

    Exemplo de URL da API:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: Data inicial para buscar os dados.
        end: Data final para buscar os dados. Se None, retorna dados até a
            data mais recente disponível.

    Returns:
        DataFrame contendo colunas Date e Value com a taxa SELIC Meta, ou
        DataFrame vazio se dados não estiverem disponíveis.

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
    if any_is_empty(start):  # Start must be provided
        return pl.DataFrame()
    df = _fetch_data_from_url(BCSerie.SELIC_TARGET, start, end)
    df = df.with_columns(pl.col("Value").round(DECIMAL_PLACES_ANNUALIZED))
    return df


def selic_target(date: DateLike) -> float:
    """Busca o valor da taxa SELIC Meta para uma data específica.

    Função de conveniência que retorna apenas o valor (não o DataFrame) para a
    data especificada.

    Args:
        date: Data de referência para buscar a taxa SELIC Meta.

    Returns:
        Taxa SELIC Meta como float ou NaN se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_target("31-05-2024")
        0.105
    """
    if any_is_empty(date):
        return float("nan")
    df = selic_target_series(date, date)
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)


def di_over_series(
    start: DateLike,
    end: DateLike | None = None,
    annualized: bool = True,
) -> pl.DataFrame:
    """Busca a taxa DI (Depósito Interbancário) do Banco Central do Brasil.

    A taxa DI representa a taxa de juros média dos empréstimos interbancários.

    Exemplo de URL da API:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: Data inicial para buscar os dados. Se None, retorna dados desde
            a data mais antiga disponível.
        end: Data final para buscar os dados. Se None, retorna dados até a
            data mais recente disponível.
        annualized: Se True, retorna a taxa anualizada (252 dias úteis por ano),
            caso contrário retorna a taxa diária.

    Returns:
        DataFrame contendo colunas Date e Value com a taxa DI, ou DataFrame
        vazio se dados não estiverem disponíveis.

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
    df = _fetch_data_from_url(BCSerie.DI_OVER, start, end)
    if annualized:
        df = df.with_columns(
            (((pl.col("Value") + 1).pow(252)) - 1)
            .round(DECIMAL_PLACES_ANNUALIZED)
            .alias("Value")
        )

    else:
        df = df.with_columns(pl.col("Value").round(DECIMAL_PLACES_DAILY))

    return df


def di_over(date: DateLike, annualized: bool = True) -> float:
    """Busca o valor da taxa DI Over para uma data específica.

    Função de conveniência que retorna apenas o valor (não o DataFrame) para a
    data especificada.

    Args:
        date: Data de referência para buscar a taxa DI Over.
        annualized: Se True, retorna a taxa anualizada (252 dias úteis por ano),
            caso contrário retorna a taxa diária.

    Returns:
        Taxa DI Over como float ou NaN se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.di_over("31-05-2024")
        0.104

        >>> bc.di_over("28-01-2025", annualized=False)
        0.00045513
    """
    if any_is_empty(date):
        return float("nan")
    df = di_over_series(date, date, annualized)
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)
