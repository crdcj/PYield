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

from pyield import relogio
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
    titulo: str | None = None,
    incluir_historico: bool = False,
) -> pl.DataFrame:
    """Implementação técnica de busca de benchmarks de TPF.

    API pública e docstring canônica: ``pyield.tpf.benchmarks``.
    """
    dados = _buscar_json_api(incluir_historico)
    df = _parsear_df(dados)
    if df.is_empty():
        return pl.DataFrame()
    df = _processar_df(df)

    if incluir_historico:
        colunas_ordenacao = ["data_inicio", "titulo", "data_vencimento"]
    else:
        colunas_ordenacao = ["titulo", "data_vencimento"]
        hoje = relogio.hoje()
        df = df.filter(pl.lit(hoje).is_between("data_inicio", "data_fim"))

    if titulo:
        df = df.filter(pl.col("titulo") == titulo.upper())

    return df.sort(colunas_ordenacao)
