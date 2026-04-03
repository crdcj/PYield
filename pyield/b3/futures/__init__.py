import datetime as dt

import polars as pl

import pyield._internal.converters as cv
from pyield._internal.types import ArrayLike, DateLike, any_is_empty, is_collection
from pyield.b3._contracts import normalizar_codigos_contrato
from pyield.b3._validar_pregao import data_negociacao_valida
from pyield.b3.futures import historical, intraday


def futures_enrich(
    df: pl.DataFrame,
    contract_code: str,
) -> pl.DataFrame:
    """Enriquece DataFrame bruto do Price Report (PR) da B3.

    Aceita um DataFrame com colunas no schema original da B3
    (ex.: ``TradDt``, ``TckrSymb``) ou já renomeadas para o padrão
    PYield. Adiciona data de vencimento, dias úteis/corridos e
    colunas derivadas (dv01, taxa_forward) conforme o contrato.

    Args:
        df: DataFrame com dados do PR da B3.
        contract_code: Código do contrato futuro
            (ex.: "DI1", "DOL").

    Returns:
        DataFrame Polars enriquecido e ordenado.
    """
    return historical.enrich(df, contract_code)


def futures(
    date: DateLike | ArrayLike,
    contract_code: str | list[str],
) -> pl.DataFrame:
    """Busca dados de um contrato futuro da B3 para a data de referência.

    Dados obtidos do dataset PR cacheado no GitHub (disponível desde 2018).
    Para dados do pregão corrente, use ``futures_intraday``.

    Args:
        date: Data de referência para consulta ou coleção de datas.
            Quando uma coleção é fornecida, os dados são buscados para cada
            data individualmente e concatenados. Datas inválidas (feriados,
            fins de semana, futuras) são silenciosamente ignoradas.
        contract_code: Código do contrato futuro na B3 ou coleção de
            códigos. Contratos disponíveis no cache histórico:
            - Juros: DI1, DDI, FRC, FRO, DAP
            - Moedas: DOL, WDO
            - Índices: IND, WIN

    Returns:
        DataFrame Polars com os dados do contrato informado.

    Examples:
        >>> df = futures("31-05-2024", "DI1")
        >>> df = futures("31-05-2024", "DAP")

        Lista de datas:

        >>> df = futures(["29-05-2024", "31-05-2024"], "DI1")
        >>> df["data_referencia"].unique().sort().to_list()
        [datetime.date(2024, 5, 29), datetime.date(2024, 5, 31)]

        Véspera de Natal e Ano Novo não têm pregão:

        >>> futures("24-12-2024", "DI1").is_empty()
        True
        >>> futures("31-12-2024", "DI1").is_empty()
        True

        Data futura e fim de semana retornam DataFrame vazio:

        >>> import datetime as dt
        >>> amanha = dt.date.today() + dt.timedelta(days=1)
        >>> futures(amanha, "DI1").is_empty()
        True
        >>> futures("04-01-2025", "DI1").is_empty()  # sábado
        True

    """
    if any_is_empty(date, contract_code):
        return pl.DataFrame()

    codigos_contrato = normalizar_codigos_contrato(contract_code)
    if not codigos_contrato:
        return pl.DataFrame()

    if is_collection(date):
        # date é ArrayLike neste branch
        datas: ArrayLike = date  # type: ignore[assignment]
        return _buscar_varias_datas(datas, codigos_contrato)

    # date é escalar (DateLike) neste branch
    data_negociacao: dt.date = cv.converter_datas(date)  # type: ignore[assignment]
    if not data_negociacao_valida(data_negociacao):
        return pl.DataFrame()

    return historical.historical(data_negociacao, codigos_contrato)


def _buscar_varias_datas(
    datas: ArrayLike,
    codigos: list[str],
) -> pl.DataFrame:
    """Busca dados para múltiplas datas do dataset PR cacheado."""
    serie_datas = cv.converter_datas(datas)
    datas_validas = [
        d for d in serie_datas if d is not None and data_negociacao_valida(d)
    ]
    if not datas_validas:
        return pl.DataFrame()

    resultados = [
        df
        for codigo in codigos
        if not (df := historical._obter_futuros_pr(datas_validas, codigo)).is_empty()
    ]

    if not resultados:
        return pl.DataFrame()
    return pl.concat(resultados, how="diagonal_relaxed")


def futures_intraday(
    contract_code: str | list[str],
) -> pl.DataFrame:
    """Busca dados intraday de contratos futuros da B3.

    Retorna os dados mais recentes do pregão corrente, com atraso
    aproximado de 15 minutos. Para dados históricos consolidados,
    use ``futures``.

    Args:
        contract_code: Código do contrato futuro na B3 ou lista de
            códigos (ex.: 'DI1', ['DI1', 'DAP']).

    Returns:
        DataFrame Polars com dados intraday. Retorna DataFrame vazio
        fora do horário de pregão.

    Output Columns:
        * data_referencia (Date): data de negociação.
        * atualizado_as (Datetime): horário a que o dado se refere
          (com atraso de ~15 min).
        * codigo_negociacao (String): código de negociação na B3.
        * data_vencimento (Date): data de vencimento do contrato.
        * dias_uteis (Int64): dias úteis até o vencimento.
        * dias_corridos (Int64): dias corridos até o vencimento.
        * contratos_abertos (Int64): contratos em aberto.
        * numero_negocios (Int64): número de negócios.
        * volume_negociado (Int64): quantidade de contratos negociados.
        * volume_financeiro (Float64): volume financeiro bruto.
        * dv01 (Float64): variação no preço para 1bp (apenas DI1).
        * preco_ultimo (Float64): último preço (apenas DI1/DAP).
        * taxa_ajuste_anterior (Float64): taxa de ajuste do dia anterior.
        * taxa_limite_minimo (Float64): limite mínimo de variação.
        * taxa_limite_maximo (Float64): limite máximo de variação.
        * taxa_abertura (Float64): taxa de abertura.
        * taxa_minima (Float64): taxa mínima negociada.
        * taxa_media (Float64): taxa média negociada.
        * taxa_maxima (Float64): taxa máxima negociada.
        * taxa_oferta_compra (Float64): melhor oferta de compra.
        * taxa_oferta_venda (Float64): melhor oferta de venda.
        * taxa_ultima (Float64): última taxa negociada.
        * taxa_forward (Float64): taxa a termo (apenas DI1/DAP).
    """
    codigos = normalizar_codigos_contrato(contract_code)
    if not codigos:
        return pl.DataFrame()

    return intraday.intraday(codigos)


def available_dates(contract_code: str) -> pl.Series:
    """Retorna as datas de negociação disponíveis no dataset histórico PR.

    Args:
        contract_code: Código do contrato futuro na B3 (ex.: DI1, DOL).

    Returns:
        Series ordenada de datas (Date) para as quais há dados de ajuste.

    Examples:
        >>> from pyield.b3.futures import available_dates
        >>> available_dates("DI1").head(3)
        shape: (3,)
        Series: 'data_referencia' [date]
        [
            2018-01-02
            2018-01-03
            2018-01-04
        ]
    """
    return historical.listar_datas_disponiveis(contract_code)


__all__ = ["available_dates", "futures", "futures_intraday"]
