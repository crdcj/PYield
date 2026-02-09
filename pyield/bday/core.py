import datetime as dt
from typing import Literal, overload

import polars as pl

import pyield.converters as cv
import pyield.types as tp
from pyield import clock
from pyield.bday.holidays.brholidays import BrHolidays
from pyield.types import ArrayLike, DateLike

LIMITE_DIA_UTIL = 6

feriados_br = BrHolidays()
FERIADOS_ANTIGOS = feriados_br.obter_feriados(opcao_feriado="old")
FERIADOS_NOVOS = feriados_br.obter_feriados(opcao_feriado="new")
DATA_TRANSICAO = BrHolidays.DATA_TRANSICAO


def count_expr(start: pl.Expr | str | dt.date, end: pl.Expr | str | dt.date) -> pl.Expr:
    """Cria uma expressão Polars para contar dias úteis (com suporte a LazyFrame).

    Esta função foi projetada para ser usada dentro de contextos do Polars,
    como ``df.select()``, ``df.with_columns()`` ou ``df.filter()``.

    Args:
        start: Nome da coluna, expressão Polars ou data literal.
        end: Nome da coluna, expressão Polars ou data literal.

    Returns:
        Uma ``pl.Expr`` que resulta em Int64.

    Examples:
        >>> import polars as pl
        >>> from pyield.bday import count_expr
        >>> start = [dt.date(2024, 1, 1), dt.date(2024, 2, 9)]
        >>> end = [dt.date(2024, 1, 5), dt.date(2024, 2, 12)]
        >>> df = pl.DataFrame({"start": start, "end": end})
        >>> df.select(count_expr("start", "end").alias("bdays"))
        shape: (2, 1)
        ┌───────┐
        │ bdays │
        │ ---   │
        │ i64   │
        ╞═══════╡
        │ 3     │
        │ 1     │
        └───────┘

        Uso com literais (ex: contar dias até o fim do ano):
        >>> df.select(bdays=count_expr("start", dt.date(2024, 12, 31)))
        shape: (2, 1)
        ┌───────┐
        │ bdays │
        │ ---   │
        │ i64   │
        ╞═══════╡
        │ 252   │
        │ 224   │
        └───────┘
    """
    if isinstance(start, str):
        start = pl.col(start)
    elif isinstance(start, dt.date):
        start = pl.lit(start)
    if isinstance(end, str):
        end = pl.col(end)
    elif isinstance(end, dt.date):
        end = pl.lit(end)
    return (
        pl.when(start < DATA_TRANSICAO)
        .then(pl.business_day_count(start=start, end=end, holidays=FERIADOS_ANTIGOS))
        .otherwise(pl.business_day_count(start=start, end=end, holidays=FERIADOS_NOVOS))
        .cast(pl.Int64)
    )


@overload
def count(start: ArrayLike, end: ArrayLike | DateLike | None) -> pl.Series: ...
@overload
def count(start: DateLike | None, end: ArrayLike) -> pl.Series: ...
@overload
def count(start: DateLike, end: DateLike) -> int: ...
@overload
def count(start: DateLike, end: None) -> None: ...
@overload
def count(start: None, end: DateLike | None) -> None: ...


def count(
    start: None | DateLike | ArrayLike,
    end: None | DateLike | ArrayLike,
) -> None | int | pl.Series:
    """Conta dias úteis entre ``start`` (inclusivo) e ``end`` (exclusivo).

    Considera feriados brasileiros com seleção de regime de feriados por elemento.

    PRESERVAÇÃO DE ORDEM (crítico): A ordem de saída SEMPRE corresponde à ordem
    elemento a elemento das entradas originais. Nenhuma ordenação, deduplicação,
    alinhamento ou remodelação é realizada. Se você passar arrays, o i-ésimo
    resultado corresponde ao i-ésimo par de (``start``, ``end``) após broadcasting.
    Isso garante atribuição segura de volta ao DataFrame de origem.

    Regime de feriados: Para cada valor de ``start``, a lista de feriados (antiga vs.
    nova) é escolhida com base na data de transição 2023-12-26 (``DATA_TRANSICAO``).
    Datas de início antes da transição usam a lista antiga; datas na transição ou
    após usam a lista nova.

    Propagação de nulos: Se qualquer argumento escalar for nulo, retorna ``None``.
    Nulos dentro de arrays de entrada produzem nulos nas posições correspondentes
    do resultado.

    Tipo de retorno: Se ambas as entradas forem escalares (não-nulos), um ``int``
    é retornado; caso contrário, uma ``polars.Series`` de contagens inteiras
    (nome: 'bday_count'). Se um escalar nulo causar curto-circuito, ``None`` é
    retornado.

    Args:
        start: Data única ou coleção (limite inclusivo).
        end: Data única ou coleção (limite exclusivo).

    Returns:
        Inteiro ou ``None`` se ``start`` e ``end`` forem datas únicas, ou Series
        se qualquer um deles for um array de datas.

    Notes:
        - Esta função é um wrapper em torno de ``polars.business_day_count``.
        - A lista de feriados é determinada por linha com base na data ``start``.

    Examples:
        >>> from pyield import bday
        >>> bday.count("15-12-2023", "01-01-2024")
        10

        Total de dias úteis em janeiro e fevereiro desde o início do ano:
        >>> bday.count(start="01-01-2024", end=["01-02-2024", "01-03-2024"])
        shape: (2,)
        Series: 'bday_count' [i64]
        [
            22
            41
        ]

        Dias úteis restantes de janeiro/fevereiro até o fim do ano:
        >>> bday.count(["01-01-2024", "01-02-2024"], "01-01-2025")
        shape: (2,)
        Series: 'bday_count' [i64]
        [
            253
            231
        ]

        Total de dias úteis em janeiro e fevereiro de 2024:
        >>> bday.count(["01-01-2024", "01-02-2024"], ["01-02-2024", "01-03-2024"])
        shape: (2,)
        Series: 'bday_count' [i64]
        [
            22
            19
        ]

        Valores nulos são propagados:
        >>> bday.count(None, "01-01-2024")  # None em start

        >>> bday.count("01-01-2024", None)  # None em end

        >>> bday.count("01-01-2024", ["01-02-2024", None])  # None dentro do array
        shape: (2,)
        Series: 'bday_count' [i64]
        [
            22
            null
        ]

        >>> start_dates = ["01-01-2024", "01-02-2024", "01-03-2024"]
        >>> bday.count(start_dates, "01-01-2025")
        shape: (3,)
        Series: 'bday_count' [i64]
        [
            253
            231
            212
        ]
    """
    # Coloca as séries em um DataFrame para trabalhar com expressões em colunas
    df = pl.DataFrame(
        data={"start": cv.converter_datas(start), "end": cv.converter_datas(end)},
        schema={"start": pl.Date, "end": pl.Date},
        nan_to_null=True,
    )

    bday_count = count_expr(pl.col("start"), pl.col("end")).alias("bday_count")

    s = df.select(bday_count)["bday_count"]

    if not tp.any_is_collection(start, end):
        return s.item()

    return s


def offset_expr(
    expr: pl.Expr | str,
    n: int | pl.Expr | str,
    roll: Literal["forward", "backward"] = "forward",
) -> pl.Expr:
    """Cria uma expressão Polars para somar dias úteis.

    Ideal para operações vetorizadas em DataFrames ou LazyFrames.

    Args:
        expr: Coluna de data original.
        n: Número de dias úteis a somar. Pode ser um inteiro fixo ou outra coluna.
        roll: Como tratar a data inicial se ela cair em fim de semana/feriado.

    Returns:
        Uma ``pl.Expr`` que resulta em Date.

    Examples:
        >>> import datetime as dt
        >>> import polars as pl
        >>> from pyield.bday import offset_expr
        >>> dates = [dt.date(2023, 12, 22), dt.date(2023, 12, 29)]
        >>> offsets = [1, 5]
        >>> df = pl.DataFrame({"dt": dates, "n": offsets})

        Adicionando um valor fixo (1 dia útil):
        >>> df.select(offset_expr("dt", 1).alias("t_plus_1"))
        shape: (2, 1)
        ┌────────────┐
        │ t_plus_1   │
        │ ---        │
        │ date       │
        ╞════════════╡
        │ 2023-12-26 │
        │ 2024-01-02 │
        └────────────┘

        Adicionando uma coluna dinâmica (prazo variável por linha):
        >>> df.select(offset_expr("dt", "n").alias("vencimento"))
        shape: (2, 1)
        ┌────────────┐
        │ vencimento │
        │ ---        │
        │ date       │
        ╞════════════╡
        │ 2023-12-26 │
        │ 2024-01-08 │
        └────────────┘
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    if isinstance(n, str):
        n = pl.col(n)
    return (
        pl.when(expr < DATA_TRANSICAO)
        .then(expr.dt.add_business_days(n=n, roll=roll, holidays=FERIADOS_ANTIGOS))
        .otherwise(expr.dt.add_business_days(n=n, roll=roll, holidays=FERIADOS_NOVOS))
    )


@overload
def offset(
    dates: ArrayLike,
    offset: ArrayLike | int | None,
    roll: Literal["forward", "backward"] = ...,
) -> pl.Series: ...
@overload
def offset(
    dates: DateLike | None,
    offset: ArrayLike,
    roll: Literal["forward", "backward"] = ...,
) -> pl.Series: ...
@overload
def offset(
    dates: DateLike,
    offset: int,
    roll: Literal["forward", "backward"] = ...,
) -> dt.date: ...
@overload
def offset(
    dates: None,
    offset: int,
    roll: Literal["forward", "backward"] = ...,
) -> None: ...
@overload
def offset(
    dates: DateLike,
    offset: None,
    roll: Literal["forward", "backward"] = ...,
) -> None: ...


def offset(
    dates: DateLike | ArrayLike | None,
    offset: int | ArrayLike | None,
    roll: Literal["forward", "backward"] = "forward",
) -> dt.date | pl.Series | None:
    """Desloca data(s) por um número de dias úteis com regime de feriados brasileiro.

    A operação é realizada em duas etapas por elemento:
    1) ROLL: Se a data original cair em fim de semana ou feriado, move-a de acordo
       com ``roll`` ("forward" -> próximo dia útil; "backward" -> anterior).
    2) ADD: Aplica o ``offset`` de dias úteis com sinal (positivo avança, negativo
       retrocede, zero = permanece na data após roll).

    PRESERVAÇÃO DE ORDEM (crítico): A ordenação de saída corresponde estritamente
    ao pareamento elemento a elemento após broadcasting entre ``dates`` e ``offset``.
    Nenhuma ordenação, deduplicação ou mudança de forma ocorre. O i-ésimo resultado
    corresponde ao i-ésimo par (date, offset), permitindo atribuição segura de volta
    ao DataFrame de origem.

    Regime de feriados: Para CADA data, a lista de feriados apropriada (antiga vs.
    nova) é escolhida com base na data de transição 2023-12-26 (``TRANSITION_DATE``).
    Datas antes da transição usam a lista *antiga*; datas na transição ou após
    usam a lista *nova*.

    Semântica do roll: ``roll`` só atua quando a data original não é um dia útil
    sob seu regime. Após o roll, a adição de dias úteis subsequente é aplicada a
    partir dessa âncora. Um ``offset`` de 0 portanto retorna ou a data original
    (se já for dia útil) ou o dia útil após roll.

    Propagação de nulos: Se qualquer argumento escalar for nulo, a função faz
    curto-circuito para ``None``. Nulos dentro de arrays de entrada propagam para
    suas posições correspondentes na saída.

    Broadcasting: ``dates`` e ``offset`` podem ser escalares ou array-like. Regras
    padrão de broadcasting do Polars aplicam-se ao construir os pares por linha.

    Tipo de retorno: Se ambas as entradas forem escalares não-nulos, um
    ``datetime.date`` é retornado. Caso contrário, uma ``polars.Series`` de datas
    nomeada ``'adjusted_date'`` é produzida. Entradas escalares nulas resultam
    em ``None``.

    Args:
        dates: Data única ou coleção de datas a serem ajustadas (roll, se necessário)
            e então deslocadas. Cada data seleciona independentemente o regime de
            feriados.
        offset: Contagem com sinal de dias úteis a aplicar após o roll. Positivo
            move para frente, negativo para trás, zero mantém a âncora após roll.
        roll: Direção para ajustar uma data inicial não-útil ("forward" ou
            "backward"). Padrão é "forward".

    Returns:
        Um ``date`` Python para entradas escalares, uma Series Polars de datas para
        qualquer entrada de array, ou ``None`` se um argumento escalar nulo foi
        fornecido.

    Notes:
        - Wrapper em torno de ``polars.Expr.dt.add_business_days`` aplicado
          condicionalmente.
        - O regime de feriados é decidido por elemento comparando com
          ``TRANSITION_DATE``.
        - Fins de semana são sempre tratados como não-úteis.

    Examples:
        >>> from pyield import bday

        Desloca sábado antes do Natal para o próximo dia útil (terça após Natal):
        >>> bday.offset("23-12-2023", 0)
        datetime.date(2023, 12, 26)

        Desloca sexta antes do Natal (sem deslocamento pois é dia útil):
        >>> bday.offset("22-12-2023", 0)
        datetime.date(2023, 12, 22)

        Desloca para o dia útil anterior se não for útil (offset=0 e roll="backward"):

        Sem deslocamento pois é dia útil:
        >>> bday.offset("22-12-2023", 0, roll="backward")
        datetime.date(2023, 12, 22)

        Desloca para o primeiro dia útil antes de "23-12-2023":
        >>> bday.offset("23-12-2023", 0, roll="backward")
        datetime.date(2023, 12, 22)

        Avança para o próximo dia útil (offset=1 e roll="forward"):

        Desloca sexta para o próximo dia útil (sexta é pulada -> segunda):
        >>> bday.offset("27-09-2024", 1)
        datetime.date(2024, 9, 30)

        Desloca sábado para o próximo dia útil (segunda é pulada -> terça):
        >>> bday.offset("28-09-2024", 1)
        datetime.date(2024, 10, 1)

        Volta para o dia útil anterior (offset=-1 e roll="backward"):

        Desloca sexta para o dia útil anterior (sexta é pulada -> quinta):
        >>> bday.offset("27-09-2024", -1, roll="backward")
        datetime.date(2024, 9, 26)

        Desloca sábado para o dia útil anterior (sexta é pulada -> quinta):
        >>> bday.offset("28-09-2024", -1, roll="backward")
        datetime.date(2024, 9, 26)

        Lista de datas e offsets:
        >>> bday.offset(["19-09-2024", "20-09-2024"], 1)
        shape: (2,)
        Series: 'adjusted_date' [date]
        [
            2024-09-20
            2024-09-23
        ]

        >>> bday.offset("19-09-2024", [1, 2])  # lista de offsets
        shape: (2,)
        Series: 'adjusted_date' [date]
        [
            2024-09-20
            2024-09-23
        ]

        Nulos escalares propagam para None:
        >>> print(bday.offset(None, 1))
        None

        Nulo escalar propaga dentro de arrays:
        >>> bday.offset(None, [1, 2])
        shape: (2,)
        Series: 'adjusted_date' [date]
        [
            null
            null
        ]

        Nulos dentro de arrays são preservados:
        >>> bday.offset(["19-09-2024", None], 1)
        shape: (2,)
        Series: 'adjusted_date' [date]
        [
            2024-09-20
            null
        ]

        >>> dates = ["19-09-2024", "20-09-2024", "21-09-2024"]
        >>> bday.offset(dates, 1)
        shape: (3,)
        Series: 'adjusted_date' [date]
        [
            2024-09-20
            2024-09-23
            2024-09-24
        ]
    """
    # Coloca as entradas em um DataFrame para trabalhar com expressões em colunas
    df = pl.DataFrame(
        data={"dates": cv.converter_datas(dates), "offset": offset},
        schema={"dates": pl.Date, "offset": pl.Int64},
        nan_to_null=True,
    )

    # Cria a expressão condicional para aplicar a lista de feriados correta
    adjusted_date = offset_expr(pl.col("dates"), n=pl.col("offset"), roll=roll).alias(
        "adjusted_date"
    )

    # Executa a expressão e obtém a série de resultados
    s = df.select(adjusted_date)["adjusted_date"]

    if not tp.any_is_collection(dates, offset):
        return s.item()

    return s


def generate(
    start: DateLike | None = None,
    end: DateLike | None = None,
    closed: Literal["both", "left", "right", "none"] = "both",
    holiday_option: Literal["old", "new", "infer"] = "new",
) -> pl.Series:
    """Gera uma Series de dias úteis entre ``start`` e ``end``.

    Considera a lista de feriados brasileiros.

    Args:
        start: Data inicial. Se None, usa a data atual.
        end: Data final. Se None, usa a data atual.
        closed: Define quais lados do intervalo são fechados (inclusivos).
            Opções válidas: 'both', 'left', 'right', 'none'. Padrão: 'both'.
        holiday_option: Especifica a lista de feriados a considerar. Padrão: "new".
            - 'old': Usa a lista de feriados vigente antes de 2023-12-26.
            - 'new': Usa a lista de feriados vigente a partir de 2023-12-26.
            - 'infer': Seleciona com base na data ``start`` relativa à transição.

    Returns:
        Series de dias úteis (nome: 'bday').

    Examples:
        >>> from pyield import bday
        >>> bday.generate(start="22-12-2023", end="02-01-2024")
        shape: (6,)
        Series: 'bday' [date]
        [
            2023-12-22
            2023-12-26
            2023-12-27
            2023-12-28
            2023-12-29
            2024-01-02
        ]
    """
    today = clock.today()
    conv_start = cv.converter_datas(start) or today
    conv_end = cv.converter_datas(end) or today

    # Gera range completo de datas
    s = pl.date_range(conv_start, conv_end, closed=closed, eager=True).alias("bday")

    # Pega feriados aplicáveis
    feriados = feriados_br.obter_feriados(
        datas=conv_start, opcao_feriado=holiday_option
    )

    # Filtra: só dias úteis (seg-sex e não feriado)
    return s.filter((s.dt.weekday() < LIMITE_DIA_UTIL) & (~s.is_in(feriados)))


def is_business_day_expr(expr: pl.Expr | str) -> pl.Expr:
    """Cria expressão Polars para verificar se é dia útil (True/False).

    Args:
        expr: Coluna de datas ou expressão Polars.

    Returns:
        Uma ``pl.Expr`` booleana.

    Examples:
        >>> import datetime as dt
        >>> import polars as pl
        >>> from pyield.bday import is_business_day_expr
        >>> dates = [dt.date(2023, 12, 25), dt.date(2023, 12, 26)]
        >>> df = pl.DataFrame({"data": dates})

        Criando uma flag booleana:
        >>> df.with_columns(is_bd=is_business_day_expr("data"))
        shape: (2, 2)
        ┌────────────┬───────┐
        │ data       ┆ is_bd │
        │ ---        ┆ ---   │
        │ date       ┆ bool  │
        ╞════════════╪═══════╡
        │ 2023-12-25 ┆ false │
        │ 2023-12-26 ┆ true  │
        └────────────┴───────┘

        Usando para filtrar apenas dias úteis:
        >>> df.filter(is_business_day_expr("data"))
        shape: (1, 1)
        ┌────────────┐
        │ data       │
        │ ---        │
        │ date       │
        ╞════════════╡
        │ 2023-12-26 │
        └────────────┘
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    return (
        pl.when(expr < DATA_TRANSICAO)
        .then(expr.dt.is_business_day(holidays=FERIADOS_ANTIGOS))
        .otherwise(expr.dt.is_business_day(holidays=FERIADOS_NOVOS))
    )


@overload
def is_business_day(dates: None) -> None: ...
@overload
def is_business_day(dates: DateLike) -> bool: ...
@overload
def is_business_day(dates: ArrayLike) -> pl.Series: ...


def is_business_day(dates: None | DateLike | ArrayLike) -> None | bool | pl.Series:
    """Determina se data(s) são dias úteis brasileiros.

    REGIME DE FERIADOS POR LINHA: Para CADA data de entrada, a lista de feriados
    apropriada ("antiga" vs. "nova") é selecionada comparando com a data de
    transição 2023-12-26 (``TRANSITION_DATE``). Datas estritamente antes da
    transição usam a lista antiga; datas na transição ou após usam a lista nova.
    Isso espelha o comportamento de ``count`` e ``offset`` que aplicam a lógica
    de regime elemento a elemento.

    PRESERVAÇÃO DE ORDEM E FORMA: A saída preserva a ordem original dos elementos.
    Nenhuma ordenação, deduplicação, remodelação ou alinhamento é realizado; o
    i-ésimo resultado corresponde à i-ésima data fornecida após broadcasting (se
    algum broadcasting ocorreu de uma entrada escalar em outro lugar da cadeia
    de chamadas).

    PROPAGAÇÃO DE NULOS: Um argumento escalar nulo faz curto-circuito para ``None``.
    Valores nulos dentro de entradas array-like produzem nulos nas posições
    correspondentes da saída.

    TIPO DE RETORNO: Se a entrada (não-nula) resolve para um único elemento, um
    ``bool`` Python é retornado. Se esse único elemento for nulo, ``None`` é
    retornado. Caso contrário, uma ``polars.Series`` de booleanos nomeada
    ``'is_bday'`` é produzida.

    FINS DE SEMANA: Sábados e domingos nunca são dias úteis independentemente do
    regime de feriados.

    Args:
        dates: Data única ou coleção (list/tuple/ndarray/Polars/Pandas Series).
            Pode incluir nulos que propagam. Entrada escalar nula retorna ``None``.

    Returns:
        ``True`` se for dia útil, ``False`` caso contrário para entrada escalar;
        ``None`` para entrada escalar nula; ou uma Series Polars de booleanos
        (nome: ``'is_bday'``) para entradas de array.

    Examples:
        >>> from pyield import bday
        >>> bday.is_business_day("25-12-2023")  # Natal (calendário antigo)
        False
        >>> bday.is_business_day("20-11-2024")  # Dia Nacional de Zumbi (novo feriado)
        False
        >>> bday.is_business_day(["22-12-2023", "26-12-2023"])  # Períodos mistos
        shape: (2,)
        Series: 'is_bday' [bool]
        [
            true
            true
        ]

    Notes:
        - Data de transição definida em ``TRANSITION_DATE``.
        - Espelha a lógica por linha usada em ``count`` e ``offset``.
        - Fins de semana sempre avaliam como ``False``.
        - Elementos nulos propagam.
    """
    # Build DataFrame to allow conditional expression selecting the right holiday list
    df = pl.DataFrame(
        {"dates": cv.converter_datas(dates)},
        schema={"dates": pl.Date},
        nan_to_null=True,
    )

    is_bday = is_business_day_expr(pl.col("dates")).alias("is_bday")

    s = df.select(is_bday)["is_bday"]

    if not tp.any_is_collection(dates):
        return s.item()

    return s


def last_business_day() -> dt.date:
    """Retorna o último dia útil no Brasil.

    Se a data atual for um dia útil, retorna a data atual. Se for fim de semana
    ou feriado, retorna o último dia útil antes da data atual.

    Returns:
        O último dia útil no Brasil.

    Notes:
        - A determinação do último dia útil considera a lista de feriados brasileiros
          correta (antes ou depois da transição 2023-12-26) aplicável à data atual.
    """
    # Get the current date in Brazil without timezone information
    bz_today = clock.today()
    result = offset(bz_today, 0, roll="backward")
    assert isinstance(result, dt.date), (
        "Assumption violated: offset did not return a date for the current date."
    )
    return result
