import polars as pl

import pyield._internal.converters as cv
from pyield import b3, du, interpolador
from pyield._internal.data_cache import obter_dataset_cacheado
from pyield._internal.types import ArrayLike, DateLike, any_is_collection, any_is_empty
from pyield.b3.futuro import futuro_datas_disponiveis as _listar_datas


def dados(
    datas: DateLike | ArrayLike,
    inicio_mes: bool = False,
    filtrar_pre: bool = False,
) -> pl.DataFrame:
    """Obtém dados de contratos de futuros de DI para datas de negociação específicas.

    Fornece acesso aos dados de futuros de DI, permitindo ajustes nas datas de
    vencimento (para início do mês) e filtragem opcional com base nos vencimentos
    de títulos públicos prefixados (LTN e NTN-F).

    Args:
        datas: Datas de negociação para as quais obter dados de
            contratos DI.
        inicio_mes: Se True, ajusta todas as datas de vencimento para o primeiro
            dia de seus respectivos meses (ex: 2025-02-03 vira 2025-02-01).
            Padrão: False.
        filtrar_pre: Se True, filtra contratos DI para incluir apenas aqueles cujas
            datas de vencimento coincidem com vencimentos conhecidos de títulos
            públicos prefixados (LTN, NTN-F) do dataset TPF mais próximo da data
            de negociação fornecida. Padrão: False.

    Returns:
        DataFrame contendo dados de contratos de futuros de DI para as datas
        especificadas, ordenados por datas de negociação e vencimento. Retorna
        DataFrame vazio se nenhum dado for encontrado.

    Examples:
        >>> from pyield.b3 import di1
        >>> df = di1.dados(datas="16-10-2024", inicio_mes=True)
        >>> df.head(3).select(
        ...     "codigo_negociacao", "data_vencimento", "dias_uteis", "taxa_ajuste"
        ... )
        shape: (3, 4)
        ┌───────────────────┬─────────────────┬────────────┬─────────────┐
        │ codigo_negociacao ┆ data_vencimento ┆ dias_uteis ┆ taxa_ajuste │
        │ ---               ┆ ---             ┆ ---        ┆ ---         │
        │ str               ┆ date            ┆ i64        ┆ f64         │
        ╞═══════════════════╪═════════════════╪════════════╪═════════════╡
        │ DI1X24            ┆ 2024-11-01      ┆ 12         ┆ 0.10653     │
        │ DI1Z24            ┆ 2024-12-01      ┆ 31         ┆ 0.1091      │
        │ DI1F25            ┆ 2025-01-01      ┆ 52         ┆ 0.11164     │
        └───────────────────┴─────────────────┴────────────┴─────────────┘

    """
    if any_is_empty(datas):
        return pl.DataFrame()

    datas_convertidas = cv.converter_datas(datas)
    if datas_convertidas is None:
        return pl.DataFrame()
    if isinstance(datas_convertidas, pl.Series):
        datas_lista = datas_convertidas.drop_nulls().unique().sort().to_list()
    else:
        datas_lista = [datas_convertidas]

    df = b3.futuro(data=datas_lista, contrato="DI1")
    if df.is_empty():
        return df

    if filtrar_pre:
        df_tpf = (
            obter_dataset_cacheado("tpf")
            .filter(pl.col("titulo").is_in(["LTN", "NTN-F"]))
            .unique(subset=["data_vencimento", "data_referencia"])
            .select(
                data_ref_tpf=pl.col("data_referencia"),
                data_vencimento=du.deslocar_expr("data_vencimento", 0),
            )
            .sort("data_ref_tpf", "data_vencimento")
        )

        # Mapear cada data_referencia do DI para a data TPF mais próxima
        df = df.join_asof(
            df_tpf.select("data_ref_tpf").unique().sort("data_ref_tpf"),
            left_on="data_referencia",
            right_on="data_ref_tpf",
            strategy="backward",
        )

        # Filtrar apenas vencimentos que existem no TPF
        df = df.join(df_tpf, on=["data_ref_tpf", "data_vencimento"], how="inner").drop(
            "data_ref_tpf"
        )

    if inicio_mes:
        df = df.with_columns(pl.col("data_vencimento").dt.truncate("1mo"))

    return df.sort("data_referencia", "data_vencimento")


def interpolar_taxas(
    datas_referencia: DateLike | ArrayLike,
    datas_vencimento: DateLike | ArrayLike,
    extrapolar: bool = True,
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
        datas_referencia: Data(s) de negociação para as taxas.
        datas_vencimento: Data(s) de vencimento correspondentes. Deve ser
            compatível em tamanho com ``datas_referencia`` se ambos forem arrays.
        extrapolar: Se permite extrapolação além do intervalo de taxas DI
            conhecidas para uma data de negociação. Padrão: True.

    Returns:
        Series contendo as taxas DI interpoladas (como floats). Valores serão
        NaN onde interpolação não for possível (ex: sem dados DI para a data
        de negociação).

    Raises:
        ValueError: Se ``datas_referencia`` e ``datas_vencimento`` forem ambos
            array-like mas tiverem tamanhos diferentes.

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
        >>> from pyield.b3 import di1
        >>> di1.interpolar_taxas(
        ...     datas_referencia=["08-05-2025", "09-05-2025", "10-05-2025"],
        ...     datas_vencimento=["01-01-2027", "25-11-2027", "01-01-2030"],
        ... )
        shape: (3,)
        Series: 'taxa_interpolada' [f64]
        [
            0.13972
            0.134613
            null
        ]

        Interpola taxas para uma data de negociação e múltiplos vencimentos:
        >>> di1.interpolar_taxas(
        ...     datas_referencia="25-04-2025",
        ...     datas_vencimento=["01-01-2027", "01-01-2050"],
        ...     extrapolar=True,
        ... )
        shape: (2,)
        Series: 'taxa_interpolada' [f64]
        [
            0.13901
            0.13881
        ]

        >>> # Com extrapolação desabilitada, vencimentos fora do intervalo retornam null
        >>> di1.interpolar_taxas(
        ...     datas_referencia="25-04-2025",
        ...     datas_vencimento=["01-11-2027", "01-01-2050"],
        ...     extrapolar=False,
        ... )
        shape: (2,)
        Series: 'taxa_interpolada' [f64]
        [
            0.135763
            null
        ]
    """
    if any_is_empty(datas_referencia, datas_vencimento):
        return pl.Series(dtype=pl.Float64)

    df_entrada = pl.DataFrame(
        data={"data_referencia": datas_referencia, "data_vencimento": datas_vencimento}
    ).with_columns(
        data_referencia=cv.converter_datas_expr("data_referencia"),
        data_vencimento=cv.converter_datas_expr("data_vencimento"),
    )
    if df_entrada.is_empty():
        return pl.Series(dtype=pl.Float64)

    # Carrega dataset de taxas DI usando datas já convertidas do df_entrada
    datas_unicas = df_entrada["data_referencia"].drop_nulls().unique().sort().to_list()
    df_ref = b3.futuro(data=datas_unicas, contrato="DI1")
    # Retorna Series vazia se nenhuma taxa for encontrada
    if df_ref.is_empty():
        return pl.Series(dtype=pl.Float64)

    # 1. CRIA O ÍNDICE ORIGINAL AQUI
    # Isso garante que saberemos a ordem exata depois
    df_entrada = df_entrada.with_row_index("_temp_idx")

    # Inicializa taxa_interpolada como None
    df_entrada = df_entrada.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
        taxa_interpolada=None,
    )

    # Lista para armazenar os blocos processados
    blocos_processados = []

    # Itera sobre cada data de referência única
    for data_ref in df_entrada["data_referencia"].unique():
        # 1. Filtra apenas as linhas desta data (Particionamento)
        df_parcial = df_entrada.filter(pl.col("data_referencia") == data_ref)

        # 2. Busca as taxas de referência para esta data
        df_referencia = df_ref.filter(pl.col("data_referencia") == data_ref)

        # Se não houver dados de curva, adicionamos o bloco como está (com nulos)
        # e continuamos.
        if df_referencia.is_empty():
            blocos_processados.append(df_parcial)
            continue

        # Inicializa o interpolador com taxas e dias úteis conhecidos
        interpolador_du = interpolador.Interpolador(
            dias_uteis=df_referencia["dias_uteis"],
            taxas=df_referencia["taxa_ajuste"],
            metodo="flat_forward",
            extrapolar=extrapolar,
        )

        df_parcial = df_parcial.with_columns(
            pl.col("dias_uteis")
            .map_elements(interpolador_du, return_dtype=pl.Float64)
            .alias("taxa_interpolada")
        )

        blocos_processados.append(df_parcial)

    if not blocos_processados:
        return pl.Series(dtype=pl.Float64)

    # 2. CONCATENA E ORDENA DE VOLTA
    # O sort("_temp_idx") restaura a ordem original dos inputs
    df_saida = pl.concat(blocos_processados).sort("_temp_idx")

    return df_saida["taxa_interpolada"].fill_nan(None)


def interpolar_taxa(
    data_referencia: DateLike,
    data_vencimento: DateLike,
    extrapolar: bool = False,
) -> float:
    """Interpola ou obtém a taxa DI para uma única data de vencimento.

    Busca dados de contratos DI para a data de negociação especificada e determina
    a taxa de liquidação para o vencimento fornecido. Se existir uma correspondência
    exata para a data de vencimento, sua taxa é retornada. Caso contrário, a taxa
    é interpolada usando o método flat-forward baseado nas taxas dos contratos
    adjacentes.

    Args:
        data_referencia: Data de negociação para a qual obter dados de DI.
        data_vencimento: Data de vencimento alvo para a taxa.
        extrapolar: Se True, permite extrapolação se o ``data_vencimento`` estiver
            fora do intervalo de vencimentos de contratos disponíveis para a
            ``data_referencia``. Padrão: False.

    Returns:
        Taxa de liquidação DI exata ou interpolada para a data e vencimento
        especificados. Retorna ``float("nan")`` se:
        - ``data_referencia`` ou ``data_vencimento`` for nulo.
        - Não há dados DI para a ``data_referencia``.
        - O ``data_vencimento`` está fora do intervalo e ``extrapolar`` é False.
        - O cálculo de interpolação falhou.

    Examples:
        >>> from pyield.b3 import di1
        >>> # Obtém taxa para um vencimento de contrato existente
        >>> di1.interpolar_taxa("25-04-2025", "01-01-2027")
        0.13901

        >>> # Obtém taxa para um vencimento não existente
        >>> di1.interpolar_taxa("25-04-2025", "01-11-2027")
        0.13576348733268917

        >>> # Extrapola taxa para uma data de vencimento futura
        >>> di1.interpolar_taxa("25-04-2025", "01-01-2050", extrapolar=True)
        0.13881
    """
    if any_is_collection(data_referencia, data_vencimento):
        raise ValueError(
            "As entradas 'data_referencia' e 'data_vencimento' devem ser datas escalares."
        )

    taxa = interpolar_taxas(
        datas_referencia=data_referencia,
        datas_vencimento=data_vencimento,
        extrapolar=extrapolar,
    )
    if taxa.is_empty():
        return float("nan")

    valor = taxa.item()
    if valor is None:
        return float("nan")

    return valor


def datas_disponiveis() -> pl.Series:
    """Retorna as datas de negociação disponíveis para DI1.

    Obtém valores distintos de 'data_referencia' para DI1, com base no dataset
    histórico PR da B3 (mesmo utilizado por `futuro`). Inclui apenas datas com
    preço e taxa de ajuste já definidos; o pregão do dia corrente não está
    incluído.

    Returns:
        Series ordenada de datas de negociação (dt.date) para as quais dados de
        ajuste de DI estão disponíveis.

    Examples:
        >>> from pyield.b3 import di1
        >>> # Série disponível no dataset PR começa em 2018-01-02
        >>> di1.datas_disponiveis().head(5)
        shape: (5,)
        Series: 'data_referencia' [date]
        [
            2018-01-02
            2018-01-03
            2018-01-04
            2018-01-05
            2018-01-08
        ]
    """
    return _listar_datas("DI1")
