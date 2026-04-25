"""Séries do Sistema Gerenciador de Séries (SGS) do Banco Central.

Séries disponíveis:
    - PTAX Venda (SGS 1)
    - SELIC Meta (SGS 432)
    - SELIC Over (SGS 1178)

Exemplos de chamada à API:
    https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=29/01/2025&dataFinal=31/01/2025
    https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados/ultimos/5?formato=json

Exemplo de resposta JSON da API do BCB:
    [{"data":"29/01/2025","valor":"12.15"},
     {"data":"30/01/2025","valor":"13.15"},
     {"data":"31/01/2025","valor":"13.15"}]

Notas de implementação:
    - Intervalos > 10 anos são divididos automaticamente em blocos.
    - SELIC Over e Meta: valores percentuais convertidos para decimal
      (divididos por 100) e arredondados para 4 casas decimais.
    - PTAX Venda: valor absoluto em R$ arredondado para 4 casas.
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

URL_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."

ESQUEMA_BRUTO = {"data": pl.Date, "valor": pl.Float64}

# Limite de segurança em dias, correspondendo a ~9.5 anos.
# Evita a complexidade do cálculo exato de 10 anos-calendário.
LIMITE_DIAS_SEGURO = 3500  # aprox 365 * 9.5

CASAS_DECIMAIS_TAXA = 4  # Selic: 2 casas no formato percentual → 4 em decimal
CASAS_DECIMAIS_PTAX = 4


class SerieSGS(Enum):
    """Enum para as séries disponíveis no SGS do Banco Central."""

    PTAX_VENDA = 1
    SELIC_META = 432
    SELIC_OVER = 1178


# ── Infraestrutura de acesso à API SGS ──────────────────────────────


@ttl_cache()
@retry_padrao
def _chamar_api(url_api: str) -> list[dict[str, str]]:
    resposta = requests.get(url_api, timeout=30)
    resposta.raise_for_status()
    return resposta.json()


def _montar_url_intervalo(
    serie: SerieSGS, inicio: dt.date, fim: dt.date | None = None
) -> str:
    inicio_str = inicio.strftime("%d/%m/%Y")
    url = f"{URL_BASE}{serie.value}/dados?formato=json&dataInicial={inicio_str}"
    if fim:
        url += f"&dataFinal={fim.strftime('%d/%m/%Y')}"
    return url


def _montar_url_ultimos(serie: SerieSGS, n: int) -> str:
    return f"{URL_BASE}{serie.value}/dados/ultimos/{n}?formato=json"


def _buscar_api(url_api: str) -> pl.DataFrame:
    """Busca dados da API e retorna DataFrame bruto {data, valor}."""
    try:
        dados = _chamar_api(url_api)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:  # noqa
            return pl.DataFrame(schema=ESQUEMA_BRUTO)
        raise

    if not dados:
        return pl.DataFrame(schema=ESQUEMA_BRUTO)

    return pl.from_dicts(dados).select(
        pl.col("data").str.to_date("%d/%m/%Y"),
        pl.col("valor").cast(pl.Float64),
    )


def _buscar_dados_url(
    serie: SerieSGS,
    inicio: DateLike,
    fim: DateLike | None = None,
) -> pl.DataFrame:
    """Orquestra a busca, dividindo intervalos > 10 anos em blocos."""
    data_inicio = converter_datas(inicio)
    data_fim = converter_datas(fim) if fim else relogio.hoje()

    if (data_fim - data_inicio).days < LIMITE_DIAS_SEGURO:
        return _buscar_api(_montar_url_intervalo(serie, data_inicio, data_fim))

    inicios = pl.date_range(start=data_inicio, end=data_fim, interval="10y", eager=True)
    fins = inicios.dt.offset_by("10y").clip(upper_bound=data_fim)

    todos_dfs = [
        _buscar_api(_montar_url_intervalo(serie, ini, fim))
        for ini, fim in zip(inicios, fins)
    ]

    todos_dfs = [df for df in todos_dfs if not df.is_empty()]

    if not todos_dfs:
        return pl.DataFrame(schema=ESQUEMA_BRUTO)

    return pl.concat(todos_dfs).unique(subset=["data"], keep="first").sort("data")


def _buscar_serie(
    serie: SerieSGS,
    inicio: DateLike | None,
    fim: DateLike | None,
    ultimos: int | None,
) -> pl.DataFrame:
    """Busca genérica para qualquer série SGS."""
    if ultimos is not None:
        return _buscar_api(_montar_url_ultimos(serie, ultimos))
    if inicio is not None:
        return _buscar_dados_url(serie, inicio, fim)
    raise ValueError("Informe 'inicio' ou 'ultimos'.")


# ── Helpers de transformação ─────────────────────────────────────────

ESQUEMA_TAXA = {"data": pl.Date, "taxa": pl.Float64}
ESQUEMA_PTAX = {"data": pl.Date, "cotacao": pl.Float64}


def _converter_para_taxa(df: pl.DataFrame) -> pl.DataFrame:
    """Converte valor percentual para decimal e renomeia para 'taxa'."""
    if df.is_empty():
        return pl.DataFrame(schema=ESQUEMA_TAXA)
    return df.select(
        "data",
        taxa=pl.col("valor").truediv(100).round(CASAS_DECIMAIS_TAXA),
    )


def _extrair_escalar(df: pl.DataFrame, coluna: str) -> float:
    """Extrai um valor escalar de um DataFrame ou retorna nan se vazio."""
    if df.is_empty():
        return float("nan")
    return df[coluna].item(0)


# ── SELIC Over ───────────────────────────────────────────────────────


def selic_over_serie(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    *,
    ultimos: int | None = None,
) -> pl.DataFrame:
    """Taxa SELIC Over (série SGS 1178).

    Taxa de juros média diária praticada no mercado interbancário,
    com títulos públicos como garantia.

    Args:
        inicio: Data inicial.
        fim: Data final. Se ``None``, usa a data mais recente.
        ultimos: Número de registros mais recentes a retornar.
            Mutuamente exclusivo com ``inicio``/``fim``.

    Returns:
        DataFrame com colunas data e taxa, ou DataFrame vazio.

    Examples:
        >>> import pyield as yd
        >>> # Sem dados em 26-01-2025 (domingo). Selic mudou por reunião do Copom.
        >>> yd.selic_over_serie("26-01-2025").head(5)  # Primeiras 5 linhas
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
        >>> yd.selic_over_serie("14-09-2025", "17-09-2025")
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
    return _converter_para_taxa(
        _buscar_serie(SerieSGS.SELIC_OVER, inicio, fim, ultimos)
    )


def selic_over(data: DateLike) -> float:
    """Taxa SELIC Over para uma data específica.

    Args:
        data: Data da consulta.

    Returns:
        Taxa SELIC Over ou ``nan`` se não disponível.

    Examples:
        >>> import pyield as yd
        >>> yd.selic_over("31-05-2024")
        0.104
    """
    if any_is_empty(data):
        return float("nan")
    return _extrair_escalar(selic_over_serie(data, data), "taxa")


# ── SELIC Meta ───────────────────────────────────────────────────────


def selic_meta_serie(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    *,
    ultimos: int | None = None,
) -> pl.DataFrame:
    """Taxa SELIC Meta (série SGS 432).

    Taxa de juros oficial definida pelo COPOM.

    Args:
        inicio: Data inicial.
        fim: Data final. Se ``None``, usa a data mais recente.
        ultimos: Número de registros mais recentes a retornar.
            Mutuamente exclusivo com ``inicio``/``fim``.

    Returns:
        DataFrame com colunas data e taxa, ou DataFrame vazio.

    Examples:
        >>> import pyield as yd
        >>> yd.selic_meta_serie("31-05-2024", "31-05-2024")
        shape: (1, 2)
        ┌────────────┬───────┐
        │ data       ┆ taxa  │
        │ ---        ┆ ---   │
        │ date       ┆ f64   │
        ╞════════════╪═══════╡
        │ 2024-05-31 ┆ 0.105 │
        └────────────┴───────┘
    """
    return _converter_para_taxa(
        _buscar_serie(SerieSGS.SELIC_META, inicio, fim, ultimos)
    )


def selic_meta(data: DateLike) -> float:
    """Taxa SELIC Meta para uma data específica.

    Args:
        data: Data da consulta.

    Returns:
        Taxa SELIC Meta ou ``nan`` se não disponível.

    Examples:
        >>> import pyield as yd
        >>> yd.selic_meta("31-05-2024")
        0.105
    """
    if any_is_empty(data):
        return float("nan")
    return _extrair_escalar(selic_meta_serie(data, data), "taxa")


# ── PTAX ─────────────────────────────────────────────────────────────


def ptax_serie(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    *,
    ultimos: int | None = None,
) -> pl.DataFrame:
    """Cotação PTAX de venda do dólar (série SGS 1).

    Fonte: Banco Central do Brasil (BCB). Frequência diária.

    A cotação retornada é a PTAX de **venda**, a taxa de câmbio
    oficial do dólar no Brasil, usada como referência para
    liquidação de contratos, marcação a mercado de derivativos,
    conversão de DV01 e índices de fundos cambiais.

    Args:
        inicio: Data inicial.
        fim: Data final. Se ``None``, usa a data mais recente.
        ultimos: Número de registros mais recentes a retornar.
            Mutuamente exclusivo com ``inicio``/``fim``.

    Returns:
        DataFrame com colunas data e cotacao, ou DataFrame vazio
        se não houver dados.

    Output Columns:
        * data (Date): data da cotação.
        * cotacao (Float64): cotação PTAX de venda em R$/US$.

    Notes:
        Disponível desde 28.11.1984. Refere-se às taxas administradas até
        março de 1990 e às taxas livres a partir de então (Resolução 1690,
        de 18.3.1990). A partir de março de 1992, essa taxa recebeu a
        denominação PTAX. Desde 1 de julho de 2011 (Circular 3506), a PTAX
        corresponde à média aritmética das taxas obtidas em quatro consultas
        diárias aos dealers de câmbio.

    Examples:
        >>> import pyield as yd
        >>> yd.ptax_serie("20-04-2025", "25-04-2025")
        shape: (4, 2)
        ┌────────────┬─────────┐
        │ data       ┆ cotacao │
        │ ---        ┆ ---     │
        │ date       ┆ f64     │
        ╞════════════╪═════════╡
        │ 2025-04-22 ┆ 5.7496  │
        │ 2025-04-23 ┆ 5.688   │
        │ 2025-04-24 ┆ 5.6738  │
        │ 2025-04-25 ┆ 5.6846  │
        └────────────┴─────────┘
    """
    df = _buscar_serie(SerieSGS.PTAX_VENDA, inicio, fim, ultimos)
    if df.is_empty():
        return pl.DataFrame(schema=ESQUEMA_PTAX)
    return df.select(
        "data",
        cotacao=pl.col("valor").round(CASAS_DECIMAIS_PTAX),
    )


def ptax(data: DateLike) -> float:
    """Cotação PTAX de venda para uma data específica.

    Retorna a PTAX de venda, taxa de câmbio oficial do dólar no
    Brasil.

    Args:
        data: Data desejada.

    Returns:
        Cotação PTAX de venda em R$/US$, ou ``nan`` se não houver
        cotação (feriado, fim de semana ou data futura).

    Examples:
        >>> import pyield as yd
        >>> yd.ptax("22-04-2025")
        5.7496

        >>> yd.ptax("20-04-2025")
        nan
    """
    if any_is_empty(data):
        return float("nan")
    return _extrair_escalar(ptax_serie(data, data), "cotacao")
