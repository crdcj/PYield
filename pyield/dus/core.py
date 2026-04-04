import datetime as dt
from typing import Literal, overload

import polars as pl

import pyield._internal.converters as cv
import pyield._internal.types as tp
from pyield import relogio
from pyield._internal.types import ArrayLike, DateLike
from pyield.dus.feriados.feriados_br import FeriadosBrasil

LIMITE_DIA_UTIL = 6

feriados_br = FeriadosBrasil()
FERIADOS_ANTIGOS = feriados_br.obter_feriados(opcao_feriado="antigo")
FERIADOS_NOVOS = feriados_br.obter_feriados(opcao_feriado="novo")
DATA_TRANSICAO = FeriadosBrasil.DATA_TRANSICAO


def _expressao_feriados(expr_data: pl.Expr) -> pl.Expr:
    return (
        pl.when(expr_data < DATA_TRANSICAO)
        .then(pl.lit(FERIADOS_ANTIGOS))
        .otherwise(pl.lit(FERIADOS_NOVOS))
    )


def contar_expr(
    inicio: pl.Expr | str | dt.date, fim: pl.Expr | str | dt.date
) -> pl.Expr:
    """Cria uma expressão Polars para contar dias úteis (com suporte a LazyFrame).

    Esta função foi projetada para ser usada dentro de contextos do Polars,
    como ``df.select()``, ``df.with_columns()`` ou ``df.filter()``.

    Args:
        inicio: Nome da coluna, expressão Polars ou data literal.
        fim: Nome da coluna, expressão Polars ou data literal.

    Returns:
        Uma ``pl.Expr`` que resulta em Int64.

    Examples:
        >>> import polars as pl
        >>> from pyield import dus
        >>> inicio = [dt.date(2024, 1, 1), dt.date(2024, 2, 9)]
        >>> fim = [dt.date(2024, 1, 5), dt.date(2024, 2, 12)]
        >>> df = pl.DataFrame({"inicio": inicio, "fim": fim})
        >>> df.select(dus.contar_expr("inicio", "fim").alias("dias_uteis"))
        shape: (2, 1)
        ┌────────────┐
        │ dias_uteis │
        │ ---        │
        │ i64        │
        ╞════════════╡
        │ 3          │
        │ 1          │
        └────────────┘

        Uso com literais (ex: contar dias até o fim do ano):
        >>> df.select(dias_uteis=dus.contar_expr("inicio", dt.date(2024, 12, 31)))
        shape: (2, 1)
        ┌────────────┐
        │ dias_uteis │
        │ ---        │
        │ i64        │
        ╞════════════╡
        │ 252        │
        │ 224        │
        └────────────┘
    """
    if isinstance(inicio, dt.date):
        data_inicio = pl.lit(inicio)
    else:
        data_inicio = cv.converter_datas_expr(inicio)

    if isinstance(fim, dt.date):
        data_fim = pl.lit(fim)
    else:
        data_fim = cv.converter_datas_expr(fim)

    return pl.business_day_count(
        start=data_inicio,
        end=data_fim,
        holidays=_expressao_feriados(data_inicio),
    ).cast(pl.Int64)


@overload
def contar(inicio: ArrayLike, fim: ArrayLike | DateLike | None) -> pl.Series: ...
@overload
def contar(inicio: DateLike | None, fim: ArrayLike) -> pl.Series: ...
@overload
def contar(inicio: DateLike, fim: DateLike) -> int: ...
@overload
def contar(inicio: DateLike, fim: None) -> None: ...
@overload
def contar(inicio: None, fim: DateLike | None) -> None: ...


def contar(
    inicio: None | DateLike | ArrayLike,
    fim: None | DateLike | ArrayLike,
) -> None | int | pl.Series:
    """Conta dias úteis entre ``inicio`` (inclusivo) e ``fim`` (exclusivo).

    Considera feriados brasileiros com seleção de regime de feriados por elemento.

    PRESERVAÇÃO DE ORDEM (crítico): A ordem de saída SEMPRE corresponde à ordem
    elemento a elemento das entradas originais. Nenhuma ordenação, deduplicação,
    alinhamento ou remodelação é realizada. Se você passar arrays, o i-ésimo
    resultado corresponde ao i-ésimo par de (``inicio``, ``fim``) após expansão.
    Isso garante atribuição segura de volta ao DataFrame de origem.

    Regime de feriados: Para cada valor de ``inicio``, a lista de feriados (antiga vs.
    nova) é escolhida com base na data de transição 2023-12-26 (``DATA_TRANSICAO``).
    Datas de início antes da transição usam a lista antiga; datas na transição ou
    após usam a lista nova.

    Propagação de nulos: Se qualquer argumento escalar for nulo, retorna ``None``.
    Nulos dentro de arrays de entrada produzem nulos nas posições correspondentes
    do resultado.

    Tipo de retorno: Se ambas as entradas forem escalares (não-nulos), um ``int``
    é retornado; caso contrário, uma ``polars.Series`` de contagens inteiras
    (nome: 'dias_uteis'). Se um escalar nulo causar curto-circuito, ``None`` é
    retornado.

    Args:
        inicio: Data única ou coleção (limite inclusivo).
        fim: Data única ou coleção (limite exclusivo).

    Returns:
        Inteiro ou ``None`` se ``inicio`` e ``fim`` forem datas únicas, ou Series
        se qualquer um deles for um array de datas.

    Notes:
        - Esta função é um encapsulamento de ``polars.business_day_count``.
        - A lista de feriados é determinada por linha com base na data ``inicio``.
        - Strings de data aceitas: ``DD-MM-YYYY``, ``DD/MM/YYYY`` e ``YYYY-MM-DD``.
        - Strings inválidas são tratadas como ``null`` e propagadas ao resultado.

    Examples:
        >>> from pyield import dus
        >>> dus.contar("15-12-2023", "01-01-2024")
        10

        Contagem negativa quando ``inicio`` é posterior a ``fim``:
        >>> dus.contar("08-01-2023", "01-01-2023")
        -5

        Total de dias úteis em janeiro e fevereiro desde o início do ano:
        >>> dus.contar(inicio="01-01-2024", fim=["01-02-2024", "01-03-2024"])
        shape: (2,)
        Series: 'dias_uteis' [i64]
        [
            22
            41
        ]

        Dias úteis restantes de janeiro/fevereiro até o fim do ano:
        >>> dus.contar(["01-01-2024", "01-02-2024"], "01-01-2025")
        shape: (2,)
        Series: 'dias_uteis' [i64]
        [
            253
            231
        ]

        Total de dias úteis em janeiro e fevereiro de 2024:
        >>> dus.contar(["01-01-2024", "01-02-2024"], ["01-02-2024", "01-03-2024"])
        shape: (2,)
        Series: 'dias_uteis' [i64]
        [
            22
            19
        ]

        Valores nulos são propagados:
        >>> dus.contar(None, "01-01-2024")  # None em inicio

        >>> dus.contar("01-01-2024", None)  # None em fim

        >>> dus.contar("01-01-2024", ["01-02-2024", None])  # None dentro do array
        shape: (2,)
        Series: 'dias_uteis' [i64]
        [
            22
            null
        ]

        >>> datas_inicio = ["01-01-2024", "01-02-2024", "01-03-2024"]
        >>> dus.contar(datas_inicio, "01-01-2025")
        shape: (3,)
        Series: 'dias_uteis' [i64]
        [
            253
            231
            212
        ]
    """
    s = (
        pl.DataFrame(
            data={"inicio": inicio, "fim": fim},
            nan_to_null=True,
        )
        .select(dias_uteis=contar_expr("inicio", "fim"))
        .get_column("dias_uteis")
    )

    if not tp.any_is_collection(inicio, fim):
        return s.item()

    return s


def deslocar_expr(
    data: pl.Expr | str,
    deslocamento: int | pl.Expr | str,
    rolagem: Literal["forward", "backward"] = "forward",
) -> pl.Expr:
    """Cria uma expressão Polars para somar dias úteis.

    Ideal para operações vetorizadas em DataFrames ou LazyFrames.

    Args:
        data: Coluna de data original.
        deslocamento: Número de dias úteis a somar. Pode ser um inteiro fixo ou
            outra coluna.
        rolagem: Como tratar a data inicial se ela cair em fim de semana/feriado.

    Returns:
        Uma ``pl.Expr`` que resulta em Date.

    Examples:
        >>> import datetime as dt
        >>> import polars as pl
        >>> from pyield import dus
        >>> datas = [dt.date(2023, 12, 22), dt.date(2023, 12, 29)]
        >>> offsets = [1, 5]
        >>> df = pl.DataFrame({"dt": datas, "n": offsets})

        Adicionando um valor fixo (1 dia útil):
        >>> df.select(dus.deslocar_expr("dt", 1).alias("t_plus_1"))
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
        >>> df.select(dus.deslocar_expr("dt", "n").alias("vencimento"))
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
    if isinstance(data, str):
        data = pl.col(data)
    if isinstance(deslocamento, str):
        deslocamento = pl.col(deslocamento)

    data_expr = cv.converter_datas_expr(data)

    return data_expr.dt.add_business_days(
        n=deslocamento,
        roll=rolagem,
        holidays=_expressao_feriados(data_expr),
    )


@overload
def deslocar(
    datas: ArrayLike,
    deslocamento: ArrayLike | int | None,
    rolagem: Literal["forward", "backward"] = ...,
) -> pl.Series: ...
@overload
def deslocar(
    datas: DateLike | None,
    deslocamento: ArrayLike,
    rolagem: Literal["forward", "backward"] = ...,
) -> pl.Series: ...
@overload
def deslocar(
    datas: DateLike,
    deslocamento: int,
    rolagem: Literal["forward", "backward"] = ...,
) -> dt.date: ...
@overload
def deslocar(
    datas: None,
    deslocamento: int,
    rolagem: Literal["forward", "backward"] = ...,
) -> None: ...
@overload
def deslocar(
    datas: DateLike,
    deslocamento: None,
    rolagem: Literal["forward", "backward"] = ...,
) -> None: ...


def deslocar(
    datas: DateLike | ArrayLike | None,
    deslocamento: int | ArrayLike | None,
    rolagem: Literal["forward", "backward"] = "forward",
) -> dt.date | pl.Series | None:
    """Desloca data(s) por um número de dias úteis com regime de feriados brasileiro.

    A operação é realizada em duas etapas por elemento:
    1) ROLL: Se a data original cair em fim de semana ou feriado, move-a de acordo
         com ``rolagem`` ("forward" -> próximo dia útil; "backward" -> anterior).
     2) ADD: Aplica o ``deslocamento`` de dias úteis com sinal (positivo avança, negativo
       retrocede, zero = permanece na data após roll).

    PRESERVAÇÃO DE ORDEM (crítico): A ordenação de saída corresponde estritamente
    ao pareamento elemento a elemento após expansão entre ``datas`` e ``deslocamento``.
    Nenhuma ordenação, deduplicação ou mudança de forma ocorre. O i-ésimo resultado
    corresponde ao i-ésimo par (data, deslocamento), permitindo atribuição segura
    de volta ao DataFrame de origem.

    Regime de feriados: Para CADA data, a lista de feriados apropriada (antiga vs.
    nova) é escolhida com base na data de transição 2023-12-26 (``DATA_TRANSICAO``).
    Datas antes da transição usam a lista *antiga*; datas na transição ou após
    usam a lista *nova*.

    Semântica da rolagem: ``rolagem`` só atua quando a data original não é um dia útil
    sob seu regime. Após o roll, a adição de dias úteis subsequente é aplicada a
    partir dessa âncora. Um ``deslocamento`` de 0 portanto retorna ou a data original
    (se já for dia útil) ou o dia útil após roll.

    Propagação de nulos: Se qualquer argumento escalar for nulo, a função faz
    curto-circuito para ``None``. Nulos dentro de arrays de entrada propagam para
    suas posições correspondentes na saída.

    Expansão: ``datas`` e ``deslocamento`` podem ser escalares ou array-like. Regras
    padrão de expansão do Polars aplicam-se ao construir os pares por linha.

    Tipo de retorno: Se ambas as entradas forem escalares não-nulos, um
    ``datetime.date`` é retornado. Caso contrário, uma ``polars.Series`` de datas
    nomeada ``'data_ajustada'`` é produzida. Entradas escalares nulas resultam
    em ``None``.

    Args:
        datas: Data única ou coleção de datas a serem ajustadas (roll, se necessário)
            e então deslocadas. Cada data seleciona independentemente o regime de
            feriados.
        deslocamento: Contagem com sinal de dias úteis a aplicar após o roll. Positivo
            move para frente, negativo para trás, zero mantém a âncora após roll.
        rolagem: Direção para ajustar uma data inicial não-útil ("forward" ou
            "backward"). Padrão é "forward".

    Returns:
        Um ``date`` Python para entradas escalares, uma Series Polars de datas para
        qualquer entrada de array, ou ``None`` se um argumento escalar nulo foi
        fornecido.

    Notes:
        - Encapsulamento de ``polars.Expr.dt.add_business_days`` aplicado
          condicionalmente.
        - O regime de feriados é decidido por elemento comparando com
          ``DATA_TRANSICAO``.
        - Fins de semana são sempre tratados como não-úteis.
        - Strings de data aceitas: ``DD-MM-YYYY``, ``DD/MM/YYYY`` e ``YYYY-MM-DD``.
        - Strings inválidas são tratadas como ``null`` e propagadas ao resultado.

    Examples:
        >>> from pyield import dus

        Desloca sábado antes do Natal para o próximo dia útil (terça após Natal):
        >>> dus.deslocar("23-12-2023", 0)
        datetime.date(2023, 12, 26)

        Desloca sexta antes do Natal (sem deslocamento pois é dia útil):
        >>> dus.deslocar("22-12-2023", 0)
        datetime.date(2023, 12, 22)

        Desloca para o dia útil anterior se não for útil (deslocamento=0 e
        rolagem="backward"):

        Sem deslocamento pois é dia útil:
        >>> dus.deslocar("22-12-2023", 0, rolagem="backward")
        datetime.date(2023, 12, 22)

        Desloca para o primeiro dia útil antes de "23-12-2023":
        >>> dus.deslocar("23-12-2023", 0, rolagem="backward")
        datetime.date(2023, 12, 22)

        Avança para o próximo dia útil (deslocamento=1 e rolagem="forward"):

        Desloca sexta para o próximo dia útil (sexta é pulada -> segunda):
        >>> dus.deslocar("27-09-2024", 1)
        datetime.date(2024, 9, 30)

        Desloca sábado para o próximo dia útil (segunda é pulada -> terça):
        >>> dus.deslocar("28-09-2024", 1)
        datetime.date(2024, 10, 1)

        Volta para o dia útil anterior (deslocamento=-1 e rolagem="backward"):

        Desloca sexta para o dia útil anterior (sexta é pulada -> quinta):
        >>> dus.deslocar("27-09-2024", -1, rolagem="backward")
        datetime.date(2024, 9, 26)

        Desloca sábado para o dia útil anterior (sexta é pulada -> quinta):
        >>> dus.deslocar("28-09-2024", -1, rolagem="backward")
        datetime.date(2024, 9, 26)

        Lista de datas e deslocamentos:
        >>> dus.deslocar(["19-09-2024", "20-09-2024"], 1)
        shape: (2,)
        Series: 'data_ajustada' [date]
        [
            2024-09-20
            2024-09-23
        ]

        >>> dus.deslocar("19-09-2024", [1, 2])  # lista de deslocamentos
        shape: (2,)
        Series: 'data_ajustada' [date]
        [
            2024-09-20
            2024-09-23
        ]

        Nulos escalares propagam para None:
        >>> print(dus.deslocar(None, 1))
        None

        Nulo escalar propaga dentro de arrays:
        >>> dus.deslocar(None, [1, 2])
        shape: (2,)
        Series: 'data_ajustada' [date]
        [
            null
            null
        ]

        Nulos dentro de arrays são preservados:
        >>> dus.deslocar(["19-09-2024", None], 1)
        shape: (2,)
        Series: 'data_ajustada' [date]
        [
            2024-09-20
            null
        ]

        >>> datas = ["19-09-2024", "20-09-2024", "21-09-2024"]
        >>> dus.deslocar(datas, 1)
        shape: (3,)
        Series: 'data_ajustada' [date]
        [
            2024-09-20
            2024-09-23
            2024-09-24
        ]
    """
    s = (
        pl.DataFrame(
            data={"datas": datas, "deslocamento": deslocamento},
            nan_to_null=True,
        )
        .select(
            data_ajustada=deslocar_expr(
                "datas", deslocamento="deslocamento", rolagem=rolagem
            )
        )
        .get_column("data_ajustada")
    )

    if not tp.any_is_collection(datas, deslocamento):
        return s.item()

    return s


def gerar(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    fechamento: Literal["both", "left", "right", "none"] = "both",
    opcao_feriado: Literal["antigo", "novo", "inferir"] = "novo",
) -> pl.Series:
    """Gera uma Series de dias úteis entre ``inicio`` e ``fim``.

    Considera a lista de feriados brasileiros.

    Args:
        inicio: Data inicial. Se None, usa a data atual.
        fim: Data final. Se None, usa a data atual.
        fechamento: Define quais lados do intervalo são fechados (inclusivos).
            Opções válidas: 'both', 'left', 'right', 'none'. Padrão: 'both'.
        opcao_feriado: Especifica a lista de feriados a considerar. Padrão: "novo".
            - 'antigo': Usa a lista de feriados vigente antes de 2023-12-26.
            - 'novo': Usa a lista de feriados vigente a partir de 2023-12-26.
            - 'inferir': Seleciona com base na data ``inicio`` relativa à transição.

    Returns:
        Series de dias úteis (nome: 'data').

    Notes:
        - Strings de data aceitas: ``DD-MM-YYYY``, ``DD/MM/YYYY`` e ``YYYY-MM-DD``.
        - ``inicio`` e ``fim`` nulos usam a data atual.

    Examples:
        >>> from pyield import dus
        >>> dus.gerar(inicio="22-12-2023", fim="02-01-2024")
        shape: (6,)
        Series: 'data' [date]
        [
            2023-12-22
            2023-12-26
            2023-12-27
            2023-12-28
            2023-12-29
            2024-01-02
        ]
    """
    hoje = relogio.hoje()
    data_inicio = cv.converter_datas(inicio) or hoje
    data_fim = cv.converter_datas(fim) or hoje

    # Gera range completo de datas
    s = pl.date_range(data_inicio, data_fim, closed=fechamento, eager=True).alias(
        "data"
    )

    # Pega feriados aplicáveis
    feriados = feriados_br.obter_feriados(
        datas=data_inicio, opcao_feriado=opcao_feriado
    )

    # Filtra: só dias úteis (seg-sex e não feriado)
    return s.filter((s.dt.weekday() < LIMITE_DIA_UTIL) & (~s.is_in(feriados)))


def e_dia_util_expr(data: pl.Expr | str) -> pl.Expr:
    """Cria expressão Polars para verificar se é dia útil (True/False).

    Args:
        data: Coluna de datas ou expressão Polars.

    Returns:
        Uma ``pl.Expr`` booleana.

    Examples:
        >>> import datetime as dt
        >>> import polars as pl
        >>> from pyield import dus
        >>> datas = [dt.date(2023, 12, 25), dt.date(2023, 12, 26)]
        >>> df = pl.DataFrame({"data": datas})

        Criando uma flag booleana:
        >>> df.with_columns(e_dia_util=dus.e_dia_util_expr("data"))
        shape: (2, 2)
        ┌────────────┬────────────┐
        │ data       ┆ e_dia_util │
        │ ---        ┆ ---        │
        │ date       ┆ bool       │
        ╞════════════╪════════════╡
        │ 2023-12-25 ┆ false      │
        │ 2023-12-26 ┆ true       │
        └────────────┴────────────┘

        Usando para filtrar apenas dias úteis:
        >>> df.filter(dus.e_dia_util_expr("data"))
        shape: (1, 1)
        ┌────────────┐
        │ data       │
        │ ---        │
        │ date       │
        ╞════════════╡
        │ 2023-12-26 │
        └────────────┘
    """
    data_expr = cv.converter_datas_expr(data)

    return data_expr.dt.is_business_day(holidays=_expressao_feriados(data_expr))


@overload
def e_dia_util(datas: None) -> None: ...
@overload
def e_dia_util(datas: DateLike) -> bool: ...
@overload
def e_dia_util(datas: ArrayLike) -> pl.Series: ...


def e_dia_util(datas: None | DateLike | ArrayLike) -> None | bool | pl.Series:
    """Determina se data(s) são dias úteis brasileiros.

    REGIME DE FERIADOS POR LINHA: Para CADA data de entrada, a lista de feriados
    apropriada ("antiga" vs. "nova") é selecionada comparando com a data de
    transição 2023-12-26 (``DATA_TRANSICAO``). Datas estritamente antes da
    transição usam a lista antiga; datas na transição ou após usam a lista nova.
    Isso espelha o comportamento de ``contar`` e ``deslocar`` que aplicam a lógica
    de regime elemento a elemento.

    PRESERVAÇÃO DE ORDEM E FORMA: A saída preserva a ordem original dos elementos.
    Nenhuma ordenação, deduplicação, remodelação ou alinhamento é realizado; o
    i-ésimo resultado corresponde à i-ésima data fornecida após expansão (se
    alguma expansão ocorreu de uma entrada escalar em outro lugar da cadeia
    de chamadas).

    PROPAGAÇÃO DE NULOS: Um argumento escalar nulo faz curto-circuito para ``None``.
    Valores nulos dentro de entradas array-like produzem nulos nas posições
    correspondentes da saída.

    TIPO DE RETORNO: Se a entrada (não-nula) resolve para um único elemento, um
    ``bool`` Python é retornado. Se esse único elemento for nulo, ``None`` é
    retornado. Caso contrário, uma ``polars.Series`` de booleanos nomeada
    ``'e_dia_util'`` é produzida.

    FINS DE SEMANA: Sábados e domingos nunca são dias úteis independentemente do
    regime de feriados.

    Args:
        datas: Data única ou coleção (list/tuple/Polars Series).
            Pode incluir nulos que propagam. Entrada escalar nula retorna ``None``.

    Returns:
        ``True`` se for dia útil, ``False`` caso contrário para entrada escalar;
        ``None`` para entrada escalar nula; ou uma Series Polars de booleanos
        (nome: ``'e_dia_util'``) para entradas de array.

    Examples:
        >>> from pyield import dus
        >>> dus.e_dia_util("25-12-2023")  # Natal (calendário antigo)
        False
        >>> dus.e_dia_util("20-11-2024")  # Dia Nacional de Zumbi (novo feriado)
        False
        >>> dus.e_dia_util(["22-12-2023", "26-12-2023"])  # Períodos mistos
        shape: (2,)
        Series: 'e_dia_util' [bool]
        [
            true
            true
        ]

    Notes:
        - Data de transição definida em ``DATA_TRANSICAO``.
        - Espelha a lógica por linha usada em ``contar`` e ``deslocar``.
        - Fins de semana sempre avaliam como ``False``.
        - Elementos nulos propagam.
        - Strings de data aceitas: ``DD-MM-YYYY``, ``DD/MM/YYYY`` e ``YYYY-MM-DD``.
        - Strings inválidas são tratadas como ``null`` e propagadas ao resultado.
    """
    s = (
        pl.DataFrame({"datas": datas}, nan_to_null=True)
        .select(e_dia_util=e_dia_util_expr("datas"))
        .get_column("e_dia_util")
    )

    if not tp.any_is_collection(datas):
        return s.item()

    return s


def ultimo_dia_util() -> dt.date:
    """Retorna o último dia útil no Brasil.

    Se a data atual for um dia útil, retorna a data atual. Se for fim de semana
    ou feriado, retorna o último dia útil antes da data atual.

    Returns:
        O último dia útil no Brasil.

    Notes:
        - A determinação do último dia útil considera a lista de feriados brasileiros
          correta (antes ou depois da transição 2023-12-26) aplicável à data atual.
    """
    # Obtém a data atual do Brasil sem informação de fuso horário
    hoje_brasil = relogio.hoje()
    result = deslocar(hoje_brasil, 0, rolagem="backward")
    assert isinstance(result, dt.date), (
        "Premissa violada: deslocar não retornou uma data para a data atual."
    )
    return result
