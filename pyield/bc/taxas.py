"""Taxas de juros do Banco Central do Brasil (séries SGS).

Séries disponíveis:
    - SELIC Over (SGS 1178)
    - SELIC Meta (SGS 432)
    - DI Over (SGS 11)

Exemplo de chamada à API:
    https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=29/01/2025&dataFinal=31/01/2025

Exemplo de resposta JSON da API do BCB (valores em percentual):
    [{"data":"29/01/2025","valor":"12.15"},
     {"data":"30/01/2025","valor":"13.15"},
     {"data":"31/01/2025","valor":"13.15"}]

Notas de implementação:
    - Valores percentuais são convertidos para decimal (divididos por 100).
    - SELIC Over e Meta: arredondadas para 4 casas decimais.
    - DI Over diária: 8 casas decimais; anualizada: 4 casas decimais.
    - Intervalos > 10 anos são divididos automaticamente em blocos.
"""

import datetime as dt
from enum import Enum

import polars as pl
import requests

from pyield import relogio
from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty


def _extrair_taxa(df: pl.DataFrame) -> float:
    """Extrai a taxa escalar de um DataFrame ou retorna nan se vazio."""
    if df.is_empty():
        return float("nan")
    return df["taxa"].item(0)


URL_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
CASAS_DECIMAIS_ANUALIZADA = 4  # 2 casas no formato percentual
CASAS_DECIMAIS_DIARIA = 8  # 6 casas no formato percentual

# Limite de segurança em dias, correspondendo a ~9.5 anos.
# Evita a complexidade do cálculo exato de 10 anos-calendário.
LIMITE_DIAS_SEGURO = 3500  # aprox 365 * 9.5


class SerieBC(Enum):
    """Enum para as séries disponíveis no Banco Central."""

    SELIC_OVER = 1178
    SELIC_META = 432
    DI_OVER = 11


@ttl_cache()
@retry_padrao
def _chamar_api(url_api: str) -> list[dict[str, str]]:
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
    esquema_esperado = {"data": pl.Date, "taxa": pl.Float64}
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
        pl.col("data").str.to_date("%d/%m/%Y"),
        taxa=pl.col("valor").cast(pl.Float64) / 100,
    )


def _buscar_dados_url(
    serie: SerieBC,
    data_inicial: DateLike,
    data_final: DateLike | None = None,
) -> pl.DataFrame:
    """Orquestra a busca, dividindo intervalos > 10 anos em blocos."""
    inicio = converter_datas(data_inicial)
    fim = converter_datas(data_final) if data_final else relogio.hoje()

    if (fim - inicio).days < LIMITE_DIAS_SEGURO:
        return _buscar_requisicao(serie, inicio, fim)

    inicios = pl.date_range(start=inicio, end=fim, interval="10y", eager=True)
    fins = inicios.dt.offset_by("10y").clip(upper_bound=fim)

    todos_dfs = [_buscar_requisicao(serie, ini, fim) for ini, fim in zip(inicios, fins)]

    todos_dfs = [df for df in todos_dfs if not df.is_empty()]

    if not todos_dfs:
        return pl.DataFrame()

    return pl.concat(todos_dfs).unique(subset=["data"], keep="first").sort("data")


def selic_over_serie(
    data_inicial: DateLike,
    data_final: DateLike | None = None,
) -> pl.DataFrame:
    """Taxa SELIC Over (série SGS 1178).

    Taxa de juros média diária praticada no mercado interbancário,
    com títulos públicos como garantia.

    Args:
        data_inicial: Data inicial.
        data_final: Data final. Se ``None``, usa a data mais recente.

    Returns:
        DataFrame com colunas data e taxa, ou DataFrame vazio.

    Examples:
        >>> from pyield import bc
        >>> # Sem dados em 26-01-2025 (domingo). Selic mudou por reunião do Copom.
        >>> bc.selic_over_serie("26-01-2025").head(5)  # Primeiras 5 linhas
        shape: (5, 2)
        ┌────────────┬────────┐
        │ data       ┆ taxa   │
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
        >>> bc.selic_over_serie("14-09-2025", "17-09-2025")
        shape: (3, 2)
        ┌────────────┬───────┐
        │ data       ┆ taxa  │
        │ ---        ┆ ---   │
        │ date       ┆ f64   │
        ╞════════════╪═══════╡
        │ 2025-09-15 ┆ 0.149 │
        │ 2025-09-16 ┆ 0.149 │
        │ 2025-09-17 ┆ 0.149 │
        └────────────┴───────┘
    """
    if any_is_empty(data_inicial):
        return pl.DataFrame()
    df = _buscar_dados_url(SerieBC.SELIC_OVER, data_inicial, data_final)
    return df.with_columns(pl.col("taxa").round(CASAS_DECIMAIS_ANUALIZADA))


def selic_over(data_referencia: DateLike) -> float:
    """Taxa SELIC Over para uma data específica.

    Args:
        data_referencia: Data de referência.

    Returns:
        Taxa SELIC Over ou ``nan`` se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_over("31-05-2024")
        0.104
    """
    if any_is_empty(data_referencia):
        return float("nan")
    return _extrair_taxa(selic_over_serie(data_referencia, data_referencia))


def selic_meta_serie(
    data_inicial: DateLike,
    data_final: DateLike | None = None,
) -> pl.DataFrame:
    """Taxa SELIC Meta (série SGS 432).

    Taxa de juros oficial definida pelo COPOM.

    Args:
        data_inicial: Data inicial.
        data_final: Data final. Se ``None``, usa a data mais recente.

    Returns:
        DataFrame com colunas data e taxa, ou DataFrame vazio.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_meta_serie("31-05-2024", "31-05-2024")
        shape: (1, 2)
        ┌────────────┬───────┐
        │ data       ┆ taxa  │
        │ ---        ┆ ---   │
        │ date       ┆ f64   │
        ╞════════════╪═══════╡
        │ 2024-05-31 ┆ 0.105 │
        └────────────┴───────┘
    """
    if any_is_empty(data_inicial):
        return pl.DataFrame()
    df = _buscar_dados_url(SerieBC.SELIC_META, data_inicial, data_final)
    return df.with_columns(pl.col("taxa").round(CASAS_DECIMAIS_ANUALIZADA))


def selic_meta(data_referencia: DateLike) -> float:
    """Taxa SELIC Meta para uma data específica.

    Args:
        data_referencia: Data de referência.

    Returns:
        Taxa SELIC Meta ou ``nan`` se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_meta("31-05-2024")
        0.105
    """
    if any_is_empty(data_referencia):
        return float("nan")
    return _extrair_taxa(selic_meta_serie(data_referencia, data_referencia))


def di_over_serie(
    data_inicial: DateLike,
    data_final: DateLike | None = None,
    anualizada: bool = True,
) -> pl.DataFrame:
    """Taxa DI Over (série SGS 11).

    Taxa de juros média dos empréstimos interbancários.

    Args:
        data_inicial: Data inicial.
        data_final: Data final. Se ``None``, usa a data mais recente.
        anualizada: Se ``True``, retorna a taxa anualizada (base
            252 d.u.). Caso contrário, retorna a taxa diária.

    Returns:
        DataFrame com colunas data e taxa, ou DataFrame vazio.

    Examples:
        >>> from pyield import bc
        >>> # Retorna todos os dados desde 29-01-2025
        >>> bc.di_over_serie("29-01-2025").head(5)  # Primeiras 5 linhas
        shape: (5, 2)
        ┌────────────┬────────┐
        │ data       ┆ taxa   │
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
    if any_is_empty(data_inicial):
        return pl.DataFrame()
    df = _buscar_dados_url(SerieBC.DI_OVER, data_inicial, data_final)
    if anualizada:
        return df.with_columns(
            taxa=(((pl.col("taxa") + 1).pow(252)) - 1).round(CASAS_DECIMAIS_ANUALIZADA)
        )
    return df.with_columns(taxa=pl.col("taxa").round(CASAS_DECIMAIS_DIARIA))


def di_over(data_referencia: DateLike, anualizada: bool = True) -> float:
    """Taxa DI Over para uma data específica.

    Args:
        data_referencia: Data de referência.
        anualizada: Se ``True``, retorna a taxa anualizada (base
            252 d.u.). Caso contrário, retorna a taxa diária.

    Returns:
        Taxa DI Over ou ``nan`` se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.di_over("31-05-2024")
        0.104

        >>> bc.di_over("28-01-2025", anualizada=False)
        0.00045513
    """
    if any_is_empty(data_referencia):
        return float("nan")
    return _extrair_taxa(di_over_serie(data_referencia, data_referencia, anualizada))