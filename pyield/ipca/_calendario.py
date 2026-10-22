"""Calendário de divulgação do IPCA publicado pelo IBGE."""

import datetime as dt
import json

import polars as pl
import requests

from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

_URL = "https://servicodados.ibge.gov.br/api/v3/calendario/9256"
_PRODUTO_IPCA = 9256


@ttl_cache()
@retry_padrao
def _buscar_calendario() -> list[bytes]:
    paginas = []
    pagina = 1
    while True:
        resposta = requests.get(_URL, params={"qtd": 100, "page": pagina}, timeout=30)
        resposta.raise_for_status()
        paginas.append(resposta.content)
        proxima = resposta.json()["nextPage"]
        if type(proxima) is not int:
            raise ValueError("Paginação inválida no calendário do IPCA.")
        if proxima == 0:
            return paginas
        if proxima <= pagina:
            raise ValueError("Paginação inválida no calendário do IPCA.")
        pagina = proxima


def _parsear_calendario(paginas: list[bytes]) -> pl.DataFrame:
    return pl.from_dicts(
        [item for pagina in paginas for item in json.loads(pagina)["items"]],
        schema={
            "produto_id": pl.Int64,
            "tipo_id": pl.Int64,
            "ano_referencia_inicio": pl.Int64,
            "mes_referencia_inicio": pl.Int64,
            "data_divulgacao": pl.String,
        },
    )


def _processar_calendario(dados: pl.DataFrame) -> pl.DataFrame:
    calendario = (
        dados.filter((pl.col("produto_id") == _PRODUTO_IPCA) & (pl.col("tipo_id") == 1))
        .select(
            competencia=pl.date(
                pl.col("ano_referencia_inicio"), pl.col("mes_referencia_inicio"), 1
            ),
            data_divulgacao=pl.col("data_divulgacao")
            .str.split(" ")
            .list.first()
            .str.to_date("%d/%m/%Y"),
        )
        .unique()
    )
    if calendario.null_count().row(0) != (0, 0):
        raise ValueError("Competência ou data ausente no calendário do IPCA.")
    if calendario["competencia"].is_duplicated().any():
        raise ValueError("Datas conflitantes no calendário do IPCA.")
    return calendario.sort("competencia")


def calendario(ano: int, *, atualizar: bool = False) -> pl.DataFrame:
    """Consulta o calendário oficial de divulgação do IPCA no IBGE.

    Args:
        ano: Ano da competência, inclusive dezembro divulgado no ano seguinte.
        atualizar: Ignora e renova o cache de 60 segundos da consulta completa.

    Returns:
        DataFrame ordenado por competência. Sem registros para o ano, retorna
        vazio com o mesmo esquema.

    Output Columns:
        competencia (Date): Primeiro dia do mês de referência do índice.
        data_divulgacao (Date): Data de divulgação prevista pelo IBGE.

    Notes:
        Fonte: API de calendário do IBGE, produto 9256. Percorre todas as
        páginas e seleciona divulgações de indicadores. A data prevista não
        comprova que o índice já foi publicado. Não estima datas ausentes.
        Erros de rede e payload inválido são propagados.

    Raises:
        ValueError: Ano inválido, paginação inválida ou datas conflitantes.
    """
    if type(ano) is not int or not dt.MINYEAR <= ano <= dt.MAXYEAR:
        raise ValueError("ano deve ser um inteiro entre 1 e 9999.")
    return _processar_calendario(
        _parsear_calendario(_buscar_calendario(_atualizar=atualizar))
    ).filter(pl.col("competencia").dt.year() == ano)
