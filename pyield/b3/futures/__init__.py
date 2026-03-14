import datetime as dt
import logging

import polars as pl

import pyield._internal.converters as cv
from pyield import clock
from pyield._internal.types import ArrayLike, DateLike, any_is_empty, is_collection
from pyield.b3._contracts import normalizar_codigos_contrato
from pyield.b3._validar_pregao import data_negociacao_valida
from pyield.b3.futures import historical, intraday

# A partir desse horário, os dados consolidados (SPR/PR) já estão disponíveis.
HORA_INICIO_CONSOLIDADO = dt.time(19, 0)

logger = logging.getLogger(__name__)


def futures(
    date: DateLike | ArrayLike,
    contract_code: str | list[str],
    full_report: bool | None = None,
) -> pl.DataFrame:
    """Busca dados de um contrato futuro da B3 para a data de referência.

    Args:
        date: Data de referência para consulta ou coleção de datas.
            Quando uma coleção é fornecida, os dados são buscados para cada
            data individualmente e concatenados. Datas inválidas (feriados,
            fins de semana, futuras) são silenciosamente ignoradas.
        contract_code: Código do contrato futuro na B3 ou coleção de códigos.
            Exemplos de códigos válidos:
            - Juros: DI1, DDI, OC1, DAP, IAP
            - Moedas: DOL, WDO, EUR, GBR, JAP, CNY
            - Índices: IND, WIN, ISP, WSP
            - Commodities: BGI, CCM, ICF, CNL, SJC, SOY, ETH, GLD
        full_report: Controla a fonte de dados quando o dado não está no cache.
            Se None (padrão) e a data é hoje: entre 09:16 e 19:00 prioriza
            dados intraday; a partir das 19:00 prioriza os dados consolidados
            (SPR → PR) com intraday como fallback.
            Se False, usa o simplified price report (SPR, ~2 KB).
            Se True, usa apenas o price report completo (PR, ~2 MB) —
            indicado para processos batch noturnos.

    Returns:
        DataFrame Polars com os dados do contrato informado.

    Notes:
        Os contratos DI1, DDI, FRC, FRO, DAP, DOL, WDO, IND e WIN possuem
        histórico pré-cacheado (desde 2018) e são retornados instantaneamente.
        Para os demais contratos, os dados são baixados diretamente da B3 a
        cada chamada, o que pode ser mais lento.

    Examples:
        >>> df = futures("31-05-2024", "DI1")
        >>> df = futures("31-05-2024", "DAP")

        Lista de datas:

        >>> df = futures(["29-05-2024", "31-05-2024"], "DI1")
        >>> df["TradeDate"].unique().sort().to_list()
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
        return _buscar_varias_datas(datas, codigos_contrato, full_report)

    # date é escalar (DateLike) neste branch
    data_negociacao: dt.date = cv.converter_datas(date)  # type: ignore[assignment]
    if not data_negociacao_valida(data_negociacao):
        return pl.DataFrame()

    return _buscar_por_fonte(data_negociacao, codigos_contrato, full_report)


def _buscar_varias_datas(
    datas: ArrayLike,
    codigos: list[str],
    full_report: bool | None,
) -> pl.DataFrame:
    """Busca dados para múltiplas datas e concatena os resultados.

    Prioriza o dataset PR cacheado (bulk). Para datas ausentes no cache,
    busca individualmente via _buscar_por_fonte.
    """
    serie_datas = cv.converter_datas(datas)
    datas_validas = [
        d for d in serie_datas if d is not None and data_negociacao_valida(d)
    ]
    if not datas_validas:
        return pl.DataFrame()

    # Bulk: carrega todas as datas de uma vez do cache, por contrato
    resultados = []
    for codigo in codigos:
        df_cache = historical.carregar_pr(datas_validas, codigo)
        if not df_cache.is_empty():
            resultados.append(df_cache)

    # Identifica datas que não vieram do cache
    datas_no_cache: set[dt.date] = set()
    for df in resultados:
        datas_no_cache.update(df["TradeDate"].unique().to_list())
    datas_faltantes = [d for d in datas_validas if d not in datas_no_cache]

    # Fallback individual apenas para datas faltantes
    for data in datas_faltantes:
        df = _buscar_por_fonte(data, codigos, full_report)
        if not df.is_empty():
            resultados.append(df)

    if not resultados:
        return pl.DataFrame()
    return pl.concat(resultados, how="diagonal_relaxed")


def _buscar_por_fonte(
    data: dt.date,
    codigos: list[str],
    full_report: bool | None,
) -> pl.DataFrame:
    """Seleciona a fonte de dados com base no horário e parâmetros."""
    if full_report is not None or data != clock.today():
        return historical.historical(data, codigos, full_report)

    horario = clock.now().time()
    if horario >= HORA_INICIO_CONSOLIDADO:
        # Consolidado (SPR → PR) primeiro; intraday como fallback
        df = historical.historical(data, codigos, full_report)
        if not df.is_empty():
            return df
        return intraday.intraday(codigos)

    if horario >= intraday.HORA_INICIO_INTRADAY:
        # Intraday primeiro; historical como fallback
        df = intraday.intraday(codigos)
        if not df.is_empty():
            return df

    return historical.historical(data, codigos, full_report)


__all__ = ["futures"]
