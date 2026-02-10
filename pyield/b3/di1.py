import datetime as dt
import logging

import polars as pl

import pyield._internal.converters as cv
from pyield import b3, bday, interpolator
from pyield._internal.data_cache import obter_dataset_cacheado
from pyield._internal.types import ArrayLike, DateLike, any_is_empty

registro = logging.getLogger(__name__)


def _carregar_com_intraday(datas: list[dt.date]) -> pl.DataFrame:
    """Busca dados de DI, incluindo dados intraday para datas ausentes no cache.

    Args:
        datas: Lista de datas a buscar.

    Returns:
        DataFrame com dados de DI para as datas solicitadas.
    """
    # 1. Busca inicial no cache com as datas solicitadas pelo usuário.
    df_cache = obter_dataset_cacheado("di1").filter(pl.col("TradeDate").is_in(datas))

    # 2. Identifica datas solicitadas que não estão no cache
    datas_solicitadas = set(datas)
    datas_cache = set(df_cache["TradeDate"].unique())
    datas_faltantes = datas_solicitadas - datas_cache

    # 3. Para cada data faltante, tenta buscar dados via API
    dfs_concat = [df_cache] if not df_cache.is_empty() else []

    for data_ref in datas_faltantes:
        try:
            df_faltante = b3.futures(contract_code="DI1", date=data_ref)

            # Só adiciona se tiver SettlementPrice
            if "SettlementPrice" in df_faltante.columns:
                dfs_concat.append(df_faltante)
            else:
                registro.warning(
                    "Dados para %s não contêm 'SettlementPrice'. Pulando esta data.",
                    data_ref,
                )
        except Exception as e:
            registro.error("Falha ao buscar dados para %s: %s", data_ref, e)

    # 4. Retorna concatenação de todos os DataFrames disponíveis
    if len(dfs_concat) == 0:
        return pl.DataFrame()  # Retorna DataFrame vazio se nada foi encontrado
    elif len(dfs_concat) == 1:
        return dfs_concat[0]
    else:
        return pl.concat(dfs_concat, how="diagonal")


def _obter_dados(datas: DateLike | ArrayLike) -> pl.DataFrame:
    datas_convertidas = cv.converter_datas(datas)

    match datas_convertidas:
        case None:
            registro.warning("Nenhuma data válida. Retornando DataFrame vazio.")
            return pl.DataFrame()
        case dt.date():
            lista_datas = [datas_convertidas]
        case pl.Series():
            lista_datas = (
                pl.Series("TradeDate", datas_convertidas).unique().sort().to_list()
            )

    df = _carregar_com_intraday(lista_datas)

    return df.sort("TradeDate", "ExpirationDate")


def data(
    dates: DateLike | ArrayLike,
    month_start: bool = False,
    pre_filter: bool = False,
) -> pl.DataFrame:
    """Obtém dados de contratos de futuros de DI para datas de negociação específicas.

    Fornece acesso aos dados de futuros de DI, permitindo ajustes nas datas de
    vencimento (para início do mês) e filtragem opcional com base nos vencimentos
    de títulos públicos prefixados (LTN e NTN-F).

    Args:
        dates: Datas de negociação para as quais obter dados de contratos DI.
        month_start: Se True, ajusta todas as datas de vencimento para o primeiro
            dia de seus respectivos meses (ex: 2025-02-03 vira 2025-02-01).
            Padrão: False.
        pre_filter: Se True, filtra contratos DI para incluir apenas aqueles cujas
            datas de vencimento coincidem com vencimentos conhecidos de títulos
            públicos prefixados (LTN, NTN-F) do dataset TPF mais próximo da data
            de negociação fornecida. Padrão: False.

    Returns:
        DataFrame contendo dados de contratos de futuros de DI para as datas
        especificadas, ordenados por datas de negociação e vencimento. Retorna
        DataFrame vazio se nenhum dado for encontrado.

    Examples:
        >>> from pyield import di1
        >>> df = di1.data(dates="16-10-2024", month_start=True)
        >>> df
        shape: (38, 22)
        ┌────────────┬────────────────┬──────────────┬───────────┬───┬─────────┬───────────┬────────────────┬─────────────┐
        │ TradeDate  ┆ ExpirationDate ┆ TickerSymbol ┆ DaysToExp ┆ … ┆ MaxRate ┆ CloseRate ┆ SettlementRate ┆ ForwardRate │
        │ ---        ┆ ---            ┆ ---          ┆ ---       ┆   ┆ ---     ┆ ---       ┆ ---            ┆ ---         │
        │ date       ┆ date           ┆ str          ┆ i64       ┆   ┆ f64     ┆ f64       ┆ f64            ┆ f64         │
        ╞════════════╪════════════════╪══════════════╪═══════════╪═══╪═════════╪═══════════╪════════════════╪═════════════╡
        │ 2024-10-16 ┆ 2024-11-01     ┆ DI1X24       ┆ 16        ┆ … ┆ 0.10656 ┆ 0.10652   ┆ 0.10653        ┆ 0.10653     │
        │ 2024-10-16 ┆ 2024-12-01     ┆ DI1Z24       ┆ 47        ┆ … ┆ 0.10914 ┆ 0.10914   ┆ 0.1091         ┆ 0.110726    │
        │ 2024-10-16 ┆ 2025-01-01     ┆ DI1F25       ┆ 78        ┆ … ┆ 0.11174 ┆ 0.11164   ┆ 0.11164        ┆ 0.1154      │
        │ 2024-10-16 ┆ 2025-02-01     ┆ DI1G25       ┆ 110       ┆ … ┆ 0.1137  ┆ 0.11365   ┆ 0.11362        ┆ 0.118314    │
        │ 2024-10-16 ┆ 2025-03-01     ┆ DI1H25       ┆ 140       ┆ … ┆ 0.11595 ┆ 0.11565   ┆ 0.1157         ┆ 0.12343     │
        │ …          ┆ …              ┆ …            ┆ …         ┆ … ┆ …       ┆ …         ┆ …              ┆ …           │
        │ 2024-10-16 ┆ 2035-01-01     ┆ DI1F35       ┆ 3730      ┆ … ┆ 0.1267  ┆ 0.1264    ┆ 0.1265         ┆ 0.124455    │
        │ 2024-10-16 ┆ 2036-01-01     ┆ DI1F36       ┆ 4095      ┆ … ┆ null    ┆ null      ┆ 0.1263         ┆ 0.124249    │
        │ 2024-10-16 ┆ 2037-01-01     ┆ DI1F37       ┆ 4461      ┆ … ┆ null    ┆ null      ┆ 0.1263         ┆ 0.1263      │
        │ 2024-10-16 ┆ 2038-01-01     ┆ DI1F38       ┆ 4828      ┆ … ┆ null    ┆ null      ┆ 0.1263         ┆ 0.1263      │
        │ 2024-10-16 ┆ 2039-01-01     ┆ DI1F39       ┆ 5192      ┆ … ┆ null    ┆ null      ┆ 0.1263         ┆ 0.1263      │
        └────────────┴────────────────┴──────────────┴───────────┴───┴─────────┴───────────┴────────────────┴─────────────┘

    """  # noqa: E501
    if any_is_empty(dates):
        registro.warning("Nenhuma data válida em 'dates'. Retornando DataFrame vazio.")
        return pl.DataFrame()
    df = _obter_dados(datas=dates)

    if pre_filter:
        df_tpf = (
            obter_dataset_cacheado("tpf")
            .filter(pl.col("BondType").is_in(["LTN", "NTN-F"]))
            .unique(subset=["MaturityDate", "ReferenceDate"])
            .select(
                TradeDate_tpf=pl.col("ReferenceDate"),
                ExpirationDate=bday.offset_expr("MaturityDate", 0),
            )
            .sort("TradeDate_tpf", "ExpirationDate")
        )

        # Ajustar as datas de vencimento para dias úteis, como está no DI1
        # exp_dates = bday.offset(df_tpf["ExpirationDate"], 0)
        # df_tpf = df_tpf.with_columns(ExpirationDate=exp_dates)

        # Mapear cada TradeDate do DI para a data TPF mais próxima
        df = df.join_asof(
            df_tpf.select("TradeDate_tpf").unique().sort("TradeDate_tpf"),
            left_on="TradeDate",
            right_on="TradeDate_tpf",
            strategy="backward",
        )

        # Filtrar apenas vencimentos que existem no TPF
        df = df.join(df_tpf, on=["TradeDate_tpf", "ExpirationDate"], how="inner").drop(
            "TradeDate_tpf"
        )

    if month_start:
        df = df.with_columns(pl.col("ExpirationDate").dt.truncate("1mo"))

    return df.sort("TradeDate", "ExpirationDate")


def interpolate_rates(
    dates: DateLike | ArrayLike,
    expirations: DateLike | ArrayLike,
    extrapolate: bool = True,
) -> pl.Series:
    """Interpola taxas de DI para datas de negociação e vencimentos especificados.

    Calcula taxas de DI interpoladas usando o método **flat-forward** para
    conjuntos de datas de negociação e vencimentos. Esta função é adequada para
    cálculos vetorizados com múltiplos pares de datas.

    Se taxas de DI não estiverem disponíveis para uma data de negociação, as
    taxas interpoladas correspondentes serão NaN.

    Trata broadcasting: Se um argumento for escalar e o outro for array, o valor
    escalar é aplicado a todos os elementos do array.

    Args:
        dates: Data(s) de negociação para as taxas.
        expirations: Data(s) de vencimento correspondentes. Deve ser compatível
            em tamanho com ``dates`` se ambos forem arrays.
        extrapolate: Se permite extrapolação além do intervalo de taxas DI
            conhecidas para uma data de negociação. Padrão: True.

    Returns:
        Series contendo as taxas DI interpoladas (como floats). Valores serão
        NaN onde interpolação não for possível (ex: sem dados DI para a data
        de negociação).

    Raises:
        ValueError: Se ``dates`` e ``expirations`` forem ambos array-like mas
            tiverem tamanhos diferentes.

    Notes:
        - Todas as taxas de liquidação disponíveis são usadas para interpolação
          flat-forward.
        - A função trata broadcasting de entradas escalares e array-like.

    Examples:
        Interpola taxas para múltiplas datas de negociação e vencimento:
        >>> # Para contrato com vencimento 01-01-2027 em 08-05-2025
        >>> # A taxa não é interpolada (taxa de liquidação é usada)
        >>> # Não há contrato com vencimento 25-11-2027 em 09-05-2025
        >>> # A taxa é interpolada (método flat-forward)
        >>> # Não há dados para 10-05-2025 (sábado) -> NaN
        >>> from pyield import di1
        >>> di1.interpolate_rates(
        ...     dates=["08-05-2025", "09-05-2025", "10-05-2025"],
        ...     expirations=["01-01-2027", "25-11-2027", "01-01-2030"],
        ... )
        shape: (3,)
        Series: 'FlatFwdRate' [f64]
        [
            0.13972
            0.134613
            null
        ]

        Interpola taxas para uma data de negociação e múltiplos vencimentos:
        >>> di1.interpolate_rates(
        ...     dates="25-04-2025",
        ...     expirations=["01-01-2027", "01-01-2050"],
        ...     extrapolate=True,
        ... )
        shape: (2,)
        Series: 'FlatFwdRate' [f64]
        [
            0.13901
            0.13881
        ]

        >>> # Com extrapolação desabilitada, vencimentos fora do intervalo retornam null
        >>> di1.interpolate_rates(
        ...     dates="25-04-2025",
        ...     expirations=["01-11-2027", "01-01-2050"],
        ...     extrapolate=False,
        ... )
        shape: (2,)
        Series: 'FlatFwdRate' [f64]
        [
            0.135763
            null
        ]
    """
    if any_is_empty(dates, expirations):
        registro.warning(
            "'dates' e 'expirations' são necessários. Retornando série vazia."
        )
        return pl.Series(dtype=pl.Float64)

    df_entrada = pl.DataFrame(
        data={"TradeDate": dates, "ExpirationDate": expirations}
    ).with_columns(
        TradeDate=cv.converter_datas_expr("TradeDate"),
        ExpirationDate=cv.converter_datas_expr("ExpirationDate"),
    )
    if df_entrada.is_empty():
        registro.warning("Entradas inválidas. Retornando Series vazia.")
        return pl.Series(dtype=pl.Float64)

    # Carrega dataset de taxas DI filtrado pelas datas de referência fornecidas
    # Usa datas já convertidas do DataFrame de entrada para evitar conversão dupla
    df_ref = _obter_dados(datas=dates)
    # Retorna Series vazia se nenhuma taxa for encontrada
    if df_ref.is_empty():
        return pl.Series(dtype=pl.Float64)

    # 1. CRIA O ÍNDICE ORIGINAL AQUI
    # Isso garante que saberemos a ordem exata depois
    df_entrada = df_entrada.with_row_index("_temp_idx")

    # Inicializa FlatFwdRate como None
    df_entrada = df_entrada.with_columns(
        BDaysToExp=bday.count_expr("TradeDate", "ExpirationDate"),
        FlatFwdRate=None,
    )

    # Lista para armazenar os blocos processados
    blocos_processados = []

    # Itera sobre cada data de referência única
    for data_ref in df_entrada["TradeDate"].unique():
        # 1. Filtra apenas as linhas desta data (Particionamento)
        df_parcial = df_entrada.filter(pl.col("TradeDate") == data_ref)

        # 2. Busca as taxas de referência para esta data
        df_referencia = df_ref.filter(pl.col("TradeDate") == data_ref)

        # Se não houver dados de curva, adicionamos o bloco como está (com nulos)
        # e continuamos.
        if df_referencia.is_empty():
            blocos_processados.append(df_parcial)
            continue

        # Inicializa o interpolador com taxas e dias úteis conhecidos
        interpolador = interpolator.Interpolator(
            method="flat_forward",
            known_bdays=df_referencia["BDaysToExp"],
            known_rates=df_referencia["SettlementRate"],
            extrapolate=extrapolate,
        )

        # 4. A mágica: map_batches passa a Series inteira para o interpolador
        # O interpolador retorna uma Series, que o Polars alinha perfeitamente
        df_parcial = df_parcial.with_columns(
            pl.col("BDaysToExp")
            .map_batches(interpolador)  # Passa Series -> Recebe Series
            .alias("FlatFwdRate")
        )

        blocos_processados.append(df_parcial)

    if not blocos_processados:
        return pl.Series(dtype=pl.Float64)

    # 2. CONCATENA E ORDENA DE VOLTA
    # O sort("_temp_idx") restaura a ordem original dos inputs
    df_saida = pl.concat(blocos_processados).sort("_temp_idx")

    return df_saida["FlatFwdRate"].fill_nan(None)


def interpolate_rate(
    date: DateLike,
    expiration: DateLike,
    extrapolate: bool = False,
) -> float:
    """Interpola ou obtém a taxa DI para uma única data de vencimento.

    Busca dados de contratos DI para a data de negociação especificada e determina
    a taxa de liquidação para o vencimento fornecido. Se existir uma correspondência
    exata para a data de vencimento, sua taxa é retornada. Caso contrário, a taxa
    é interpolada usando o método flat-forward baseado nas taxas dos contratos
    adjacentes.

    Args:
        date: Data de negociação para a qual obter dados de DI.
        expiration: Data de vencimento alvo para a taxa.
        extrapolate: Se True, permite extrapolação se o ``expiration`` estiver
            fora do intervalo de vencimentos de contratos disponíveis para a
            ``date``. Padrão: False.

    Returns:
        Taxa de liquidação DI exata ou interpolada para a data e vencimento
        especificados. Retorna ``float("nan")`` se:
        - Não há dados DI para a ``date``.
        - O ``expiration`` está fora do intervalo e ``extrapolate`` é False.
        - O cálculo de interpolação falhou.

    Examples:
        >>> from pyield import di1
        >>> # Obtém taxa para um vencimento de contrato existente
        >>> di1.interpolate_rate("25-04-2025", "01-01-2027")
        0.13901

        >>> # Obtém taxa para um vencimento não existente
        >>> di1.interpolate_rate("25-04-2025", "01-11-2027")
        0.13576348733268917

        >>> # Extrapola taxa para uma data de vencimento futura
        >>> di1.interpolate_rate("25-04-2025", "01-01-2050", extrapolate=True)
        0.13881
    """
    if any_is_empty(date, expiration):
        registro.warning(
            "As entradas 'date' e 'expiration' são obrigatórias. Retornando NaN."
        )
        return float("nan")

    data_convertida = cv.converter_datas(date)
    vencimento_convertido = cv.converter_datas(expiration)

    if not isinstance(data_convertida, dt.date) or not isinstance(
        vencimento_convertido, dt.date
    ):
        raise ValueError("As entradas 'date' e 'expiration' devem ser datas escalares.")

    # Obtém o DataFrame de contratos DI
    df_di = _obter_dados(datas=data_convertida)

    if df_di.is_empty():
        return float("nan")

    interpolador = interpolator.Interpolator(
        method="flat_forward",
        known_bdays=df_di["BDaysToExp"],
        known_rates=df_di["SettlementRate"],
        extrapolate=extrapolate,
    )

    bd = bday.count(data_convertida, vencimento_convertido)
    return interpolador(bd)


def available_trade_dates() -> pl.Series:
    """Retorna todas as datas de negociação disponíveis (completas) no dataset de DI.

    Obtém valores distintos de 'TradeDate' presentes no cache de dados históricos
    de futuros de DI, ordenados cronologicamente.

    Returns:
        Series ordenada de datas de negociação únicas (dt.date) para as quais
        dados de DI estão disponíveis.

    Examples:
        >>> from pyield import di1
        >>> # Série de futuros de DI começa em 1995-01-02
        >>> di1.available_trade_dates().head(5)
        shape: (5,)
        Series: 'available_dates' [date]
        [
            1995-01-02
            1995-01-03
            1995-01-04
            1995-01-05
            1995-01-06
        ]
    """
    datas_disponiveis = (
        obter_dataset_cacheado("di1")
        .get_column("TradeDate")
        .unique()
        .sort()
        .alias("available_dates")
    )
    return datas_disponiveis
