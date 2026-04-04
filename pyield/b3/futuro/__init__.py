import polars as pl

import pyield._internal.converters as cv
from pyield._internal.types import ArrayLike, DateLike, any_is_empty
from pyield.b3._validar_pregao import data_negociacao_valida
from pyield.b3.futuro import historico, intradia


def futuro_enriquecer(
    df: pl.DataFrame,
    codigo_contrato: str,
) -> pl.DataFrame:
    """Enriquece DataFrame bruto do Price Report (PR) da B3.

    Aceita um DataFrame com colunas no schema original da B3
    (ex.: ``TradDt``, ``TckrSymb``) ou já renomeadas para o padrão
    PYield. Adiciona data de vencimento, dias úteis/corridos e
    colunas derivadas (dv01, taxa_forward) conforme o contrato.

    Args:
        df: DataFrame com dados do PR da B3.
        codigo_contrato: Código do contrato futuro
            (ex.: "DI1", "DOL").

    Returns:
        DataFrame Polars enriquecido e ordenado.
    """
    return historico.enrich(df, codigo_contrato)


def futuro(
    data_referencia: DateLike | ArrayLike,
    codigo_contrato: str,
) -> pl.DataFrame:
    """Busca dados de um contrato futuro da B3 para a data de referência.

    Dados obtidos do dataset PR cacheado no GitHub (disponível desde 2018).
    Para dados do pregão corrente, use ``futuro_intradia``.

    Args:
        data_referencia: Data de referência para consulta ou coleção de datas.
            Quando uma coleção é fornecida, os dados são buscados para cada
            data individualmente e concatenados. Datas inválidas (feriados,
            fins de semana, futuras) são silenciosamente ignoradas.
        codigo_contrato: Código do contrato futuro na B3. Contratos
            disponíveis no cache histórico:
            - Juros: DI1, DDI, FRC, FRO, DAP
            - Moedas: DOL, WDO
            - Índices: IND, WIN

    Returns:
        DataFrame Polars com os dados do contrato informado.

    Examples:
        >>> df = futuro("31-05-2024", "DI1")
        >>> df = futuro("31-05-2024", "DAP")

        Lista de datas:

        >>> df = futuro(["29-05-2024", "31-05-2024"], "DI1")
        >>> df["data_referencia"].unique().sort().to_list()
        [datetime.date(2024, 5, 29), datetime.date(2024, 5, 31)]

        Véspera de Natal e Ano Novo não têm pregão:

        >>> futuro("24-12-2024", "DI1").is_empty()
        True
        >>> futuro("31-12-2024", "DI1").is_empty()
        True

        Data futura e fim de semana retornam DataFrame vazio:

        >>> import datetime as dt
        >>> amanha = dt.date.today() + dt.timedelta(days=1)
        >>> futuro(amanha, "DI1").is_empty()
        True
        >>> futuro("04-01-2025", "DI1").is_empty()  # sábado
        True

    """
    if any_is_empty(data_referencia, codigo_contrato):
        return pl.DataFrame()

    dados_convertidos = cv.converter_datas(data_referencia)
    if isinstance(dados_convertidos, pl.Series):
        datas_validas = []
        for d in dados_convertidos:
            if d is not None and data_negociacao_valida(d):
                datas_validas.append(d)
        return historico._buscar_do_cache(datas_validas, codigo_contrato)

    if not data_negociacao_valida(dados_convertidos):
        return pl.DataFrame()

    return historico.historical(dados_convertidos, codigo_contrato)


def futuro_intradia(
    codigo_contrato: str,
) -> pl.DataFrame:
    """Busca dados intradia de contratos futuros da B3.

    Retorna os dados mais recentes do pregão corrente, com atraso
    aproximado de 15 minutos. Para dados históricos consolidados,
    use ``futuro``.

    Args:
        codigo_contrato: Código do contrato futuro na B3
            (ex.: 'DI1', 'DAP', 'DOL').

    Returns:
        DataFrame Polars com dados intradia. Retorna DataFrame vazio
        fora do horário de pregão.

    Notes:
        As colunas com prefixo ``preco_`` aparecem para contratos cotados
        por preço (ex.: DOL, IND). As com prefixo ``taxa_`` aparecem para
        contratos cotados por taxa (ex.: DI1, DAP, DDI, FRC, FRO).
    """
    if not codigo_contrato:
        return pl.DataFrame()

    return intradia.intradia(codigo_contrato)


def futuro_datas_disponiveis(codigo_contrato: str) -> pl.Series:
    """Retorna as datas de negociação disponíveis no dataset cacheado.

    Args:
        codigo_contrato: Código do contrato futuro na B3 (ex.: DI1, DOL).

    Returns:
        Series ordenada de datas (Date) para as quais há dados de ajuste.

    Examples:
        >>> from pyield.b3.futures import futuro_datas_disponiveis
        >>> futuro_datas_disponiveis("DI1").head(3)
        shape: (3,)
        Series: 'data_referencia' [date]
        [
            2018-01-02
            2018-01-03
            2018-01-04
        ]
    """
    return historico.listar_datas_disponiveis(codigo_contrato)


__all__ = [
    "futuro",
    "futuro_datas_disponiveis",
    "futuro_enriquecer",
    "futuro_intradia",
]
