"""Benchmarks de títulos públicos brasileiros (API do Tesouro Nacional).

Exemplo de chamada à API:
    https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/benchmarks?incluir_historico=N

Exemplo de resposta JSON da API:
    {"registros": [
        {"BENCHMARK": "LFT 6 anos",
         "VENCIMENTO": "2032-03-01",
         "T\u00cdTULO": "LFT",
         "IN\u00cdCIO": "2026-01-01",
         "TERMINO": "2026-03-31"},
        {"BENCHMARK": "LTN 6 meses",
         "VENCIMENTO": "2026-10-01",
         "T\u00cdTULO": "LTN",
         "IN\u00cdCIO": "2026-01-01",
         "TERMINO": "2026-03-31"},
        ...
    ]}
"""

import logging

import polars as pl
import requests

from pyield import clock
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

registro = logging.getLogger(__name__)

URL_BASE_API = (
    "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/benchmarks"
)


@ttl_cache()
@retry_padrao
def _buscar_json_api(incluir_historico: bool) -> dict:
    """Busca os dados brutos de benchmarks na API do Tesouro Nacional."""
    param = "S" if incluir_historico else "N"
    url = f"{URL_BASE_API}?incluir_historico={param}"
    resposta = requests.get(url, timeout=10)
    resposta.raise_for_status()
    return resposta.json()


def _parsear_df(dados: dict) -> pl.DataFrame:
    registros = dados.get("registros", [])
    if not registros:
        return pl.DataFrame()
    return pl.DataFrame(registros)


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    df = df.select(
        titulo=pl.col("TÍTULO").str.strip_chars(),
        data_vencimento=pl.col("VENCIMENTO").str.to_date(strict=False),
        benchmark=pl.col("BENCHMARK").str.strip_chars(),
        data_inicio=pl.col("INÍCIO").str.to_date(strict=False),
        data_fim=pl.col("TERMINO").str.to_date(strict=False),
    )

    total_nulos = sum(df.null_count().row(0))
    if total_nulos:
        registro.warning(
            "Células nulas após parse (total=%s). Linhas descartadas.",
            total_nulos,
        )
        df = df.drop_nulls()
    return df


def benchmarks(
    bond_type: str | None = None, include_history: bool = False
) -> pl.DataFrame:
    """Busca benchmarks de títulos públicos brasileiros na API do TN.

    Recupera dados atuais ou históricos de benchmarks para títulos do
    Tesouro Nacional (ex.: LTN, LFT, NTN-B).

    Args:
        bond_type: Tipo do título a filtrar (ex.: "LFT").
        include_history: Se ``True``, inclui histórico; se ``False``
            (padrão), retorna apenas benchmarks vigentes.

    Returns:
        DataFrame com os benchmarks, ou DataFrame vazio.

    Output Columns:
        * titulo (String): tipo do título (ex.: "LTN", "LFT").
        * data_vencimento (Date): data de vencimento do benchmark.
        * benchmark (String): nome/identificador do benchmark.
        * data_inicio (Date): data de início da vigência.
        * data_fim (Date): data de término da vigência.

    Notes:
        Dados obtidos da API oficial do Tesouro Nacional.
        Documentação da API:
        https://portal-conhecimento.tesouro.gov.br/catalogo-componentes/api-leil%C3%B5es

    Examples:
        >>> from pyield import tn
        >>> df_current = tn.benchmarks()
        >>> # Benchmarks históricos
        >>> tn.benchmarks(bond_type="LFT", include_history=True).head()
        shape: (5, 5)
        ┌────────┬─────────────────┬────────────┬─────────────┬────────────┐
        │ titulo ┆ data_vencimento ┆ benchmark  ┆ data_inicio ┆ data_fim   │
        │ ---    ┆ ---             ┆ ---        ┆ ---         ┆ ---        │
        │ str    ┆ date            ┆ str        ┆ date        ┆ date       │
        ╞════════╪═════════════════╪════════════╪═════════════╪════════════╡
        │ LFT    ┆ 2020-03-01      ┆ LFT 6 anos ┆ 2014-01-01  ┆ 2014-06-30 │
        │ LFT    ┆ 2020-09-01      ┆ LFT 6 anos ┆ 2014-07-01  ┆ 2014-12-31 │
        │ LFT    ┆ 2021-03-01      ┆ LFT 6 anos ┆ 2015-01-01  ┆ 2015-04-30 │
        │ LFT    ┆ 2021-09-01      ┆ LFT 6 anos ┆ 2015-05-01  ┆ 2015-12-31 │
        │ LFT    ┆ 2022-03-01      ┆ LFT 6 anos ┆ 2016-01-01  ┆ 2016-06-30 │
        └────────┴─────────────────┴────────────┴─────────────┴────────────┘
    """
    dados = _buscar_json_api(include_history)
    df = _parsear_df(dados)
    if df.is_empty():
        return pl.DataFrame()
    df = _processar_df(df)

    if include_history:
        colunas_ordenacao = ["data_inicio", "titulo", "data_vencimento"]
    else:
        colunas_ordenacao = ["titulo", "data_vencimento"]
        hoje = clock.today()
        df = df.filter(pl.lit(hoje).is_between("data_inicio", "data_fim"))

    if bond_type:
        df = df.filter(pl.col("titulo") == bond_type.upper())

    return df.sort(colunas_ordenacao)
