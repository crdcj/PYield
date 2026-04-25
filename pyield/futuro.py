"""Contratos futuros negociados na B3."""

import polars as pl

import pyield._internal.converters as cv
from pyield._internal.types import ArrayLike, DateLike, any_is_empty
from pyield.b3._validar_pregao import data_negociacao_valida
from pyield.b3.futuro import historico as _historico
from pyield.b3.futuro import intradia as _intradia


def historico(data: DateLike | ArrayLike, contrato: str) -> pl.DataFrame:
    """Busca dados históricos de um contrato futuro.

    Args:
        data: Data da consulta ou coleção de datas.
        contrato: Contrato futuro (ex.: ``DI1``, ``DAP``, ``DOL``).

    Returns:
        DataFrame Polars com os dados do contrato informado.
    """
    if any_is_empty(data, contrato):
        return pl.DataFrame()

    dados_convertidos = cv.converter_datas(data)
    if isinstance(dados_convertidos, pl.Series):
        datas_validas = []
        for data_ref in dados_convertidos:
            if data_ref is not None and data_negociacao_valida(data_ref):
                datas_validas.append(data_ref)
        return _historico._buscar_do_cache(datas_validas, contrato)

    if not data_negociacao_valida(dados_convertidos):
        return pl.DataFrame()

    return _historico.historico(dados_convertidos, contrato)


def intradia(contrato: str) -> pl.DataFrame:
    """Busca dados intradia de um contrato futuro.

    Args:
        contrato: Contrato futuro (ex.: ``DI1``, ``DAP``, ``DOL``).

    Returns:
        DataFrame Polars com dados intradia. Retorna DataFrame vazio
        fora do horário de pregão.
    """
    if not contrato:
        return pl.DataFrame()

    return _intradia.intradia(contrato)


def enriquecer(df: pl.DataFrame, contrato: str) -> pl.DataFrame:
    """Enriquece DataFrame bruto do Price Report (PR) da B3.

    Aceita um DataFrame com colunas no schema original da B3
    (ex.: ``TradDt``, ``TckrSymb``). Filtra pelo contrato informado,
    adiciona data de vencimento, dias úteis/corridos e colunas
    derivadas (dv01, taxa_forward) conforme o contrato.

    Args:
        df: DataFrame com dados brutos do PR da B3.
        contrato: Contrato futuro (ex.: ``DI1``, ``DOL``).

    Returns:
        DataFrame Polars enriquecido e ordenado.
    """
    return _historico.enriquecer(df=df, contrato=contrato)


def datas_disponiveis(contrato: str) -> pl.Series:
    """Retorna as datas disponíveis no dataset histórico cacheado.

    Args:
        contrato: Contrato futuro (ex.: ``DI1``, ``DOL``).

    Returns:
        Series ordenada de datas para as quais há dados históricos.
    """
    return _historico.listar_datas_disponiveis(contrato)


__all__ = [
    "datas_disponiveis",
    "enriquecer",
    "historico",
    "intradia",
]
