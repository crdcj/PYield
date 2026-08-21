import datetime as dt
from typing import Literal, overload

import polars as pl

import pyield._internal.converters as cv
import pyield._internal.types as tp
from pyield import relogio
from pyield._internal.types import ArrayLike, DateLike, DatesLike
from pyield.du import feriados_br

Calendario = Literal["auto", "anterior", "atual"]
Ajuste = Literal["seguinte", "anterior"]
LimitesInclusivos = Literal["ambos", "inicio", "fim", "nenhum"]


def _traduzir_ajuste(
    ajuste: Ajuste,
) -> Literal["forward", "backward"]:
    match ajuste:
        case "seguinte":
            return "forward"
        case "anterior":
            return "backward"
        case _:
            raise ValueError("Opção inválida para ajuste.")


def _traduzir_limites_inclusivos(
    limites_inclusivos: LimitesInclusivos,
) -> Literal["both", "left", "right", "none"]:
    match limites_inclusivos:
        case "ambos":
            return "both"
        case "inicio":
            return "left"
        case "fim":
            return "right"
        case "nenhum":
            return "none"
        case _:
            raise ValueError("Opção inválida para limites_inclusivos.")


def _selecionar_feriados(
    data: dt.date,
    calendario: Calendario,
) -> list[dt.date]:
    match calendario:
        case "anterior":
            return feriados_br.ANTERIORES
        case "atual":
            return feriados_br.ATUAIS
        case "auto":
            if data < feriados_br.DATA_TRANSICAO:
                return feriados_br.ANTERIORES
            return feriados_br.ATUAIS
        case _:
            raise ValueError("Opção inválida para calendario.")


def _expressao_feriados(
    expr_data: pl.Expr,
    calendario: Calendario = "auto",
) -> pl.Expr:
    match calendario:
        case "anterior":
            return pl.lit(feriados_br.ANTERIORES)
        case "atual":
            return pl.lit(feriados_br.ATUAIS)
        case "auto":
            return (
                pl.when(expr_data < feriados_br.DATA_TRANSICAO)
                .then(pl.lit(feriados_br.ANTERIORES))
                .otherwise(pl.lit(feriados_br.ATUAIS))
            )
        case _:
            raise ValueError("Opção inválida para calendario.")


def contar_expr(
    inicio: pl.Expr | str | dt.date,
    fim: pl.Expr | str | dt.date,
    calendario: Calendario = "auto",
) -> pl.Expr:
    """Cria uma expressão Polars para contar dias úteis (com suporte a LazyFrame).

    Esta função foi projetada para ser usada dentro de contextos do Polars,
    como ``df.select()``, ``df.with_columns()`` ou ``df.filter()``.

    Args:
        inicio: Nome da coluna, expressão Polars ou data literal.
        fim: Nome da coluna, expressão Polars ou data literal.
        calendario: Lista de feriados a considerar. ``"anterior"`` usa a lista
            vigente antes de 26-12-2023, ``"atual"`` usa a lista vigente a partir
            dessa data e ``"auto"`` seleciona a lista por linha com base em
            ``inicio``. Padrão: ``"auto"``.

    Returns:
        Uma ``pl.Expr`` que resulta em Int64.

    Examples:
        >>> import polars as pl
        >>> from pyield import du
        >>> inicio = [dt.date(2024, 1, 1), dt.date(2024, 2, 9)]
        >>> fim = [dt.date(2024, 1, 5), dt.date(2024, 2, 12)]
        >>> df = pl.DataFrame({"inicio": inicio, "fim": fim})
        >>> df.select(du.contar_expr("inicio", "fim").alias("dias_uteis"))
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
        >>> df.select(dias_uteis=du.contar_expr("inicio", dt.date(2024, 12, 31)))
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
        inicio = pl.lit(inicio)
    else:
        inicio = cv.converter_datas_expr(inicio)

    if isinstance(fim, dt.date):
        fim = pl.lit(fim)
    else:
        fim = cv.converter_datas_expr(fim)

    return pl.business_day_count(
        start=inicio,
        end=fim,
        holidays=_expressao_feriados(inicio, calendario),
    ).cast(pl.Int64)


@overload
def contar(
    inicio: DatesLike,
    fim: DatesLike | DateLike | None,
    calendario: Calendario = ...,
) -> pl.Series: ...
@overload
def contar(
    inicio: DateLike | None,
    fim: DatesLike,
    calendario: Calendario = ...,
) -> pl.Series: ...
@overload
def contar(
    inicio: DateLike,
    fim: DateLike,
    calendario: Calendario = ...,
) -> int: ...
@overload
def contar(
    inicio: DateLike,
    fim: None,
    calendario: Calendario = ...,
) -> None: ...
@overload
def contar(
    inicio: None,
    fim: DateLike | None,
    calendario: Calendario = ...,
) -> None: ...


def contar(
    inicio: None | DateLike | DatesLike,
    fim: None | DateLike | DatesLike,
    calendario: Calendario = "auto",
) -> None | int | pl.Series:
    """Conta dias úteis entre ``inicio`` (inclusivo) e ``fim`` (exclusivo).

    Considera feriados brasileiros com seleção de regime de feriados por elemento.

    PRESERVAÇÃO DE ORDEM (crítico): A ordem de saída SEMPRE corresponde à ordem
    elemento a elemento das entradas originais. Nenhuma ordenação, deduplicação,
    alinhamento ou remodelação é realizada. Se você passar arrays, o i-ésimo
    resultado corresponde ao i-ésimo par de (``inicio``, ``fim``) após expansão.
    Isso garante atribuição segura de volta ao DataFrame de origem.

    Regime de feriados: Por padrão, para cada valor de ``inicio``, a lista de
    feriados (anterior vs. atual) é escolhida com base na data de transição
    26-12-2023. Também é possível selecionar explicitamente uma das listas para toda
    a contagem.

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
        calendario: Lista de feriados a considerar. ``"anterior"`` usa a lista
            vigente antes de 26-12-2023, ``"atual"`` usa a lista vigente a partir
            dessa data e ``"auto"`` seleciona a lista por elemento com base em
            ``inicio``. Padrão: ``"auto"``.

    Returns:
        Inteiro ou ``None`` se ``inicio`` e ``fim`` forem datas únicas, ou Series
        se qualquer um deles for um array de datas.

    Notes:
        - Esta função é um encapsulamento de ``polars.business_day_count``.
        - Com ``calendario="auto"``, a lista é determinada por linha com base na
          data ``inicio``.
        - Strings de data aceitas: ``DD-MM-YYYY``, ``DD/MM/YYYY`` e ``YYYY-MM-DD``.
        - Strings inválidas são tratadas como ``null`` e propagadas ao resultado.

    Examples:
        >>> from pyield import du
        >>> du.contar("15-12-2023", "01-01-2024")
        10

        Transição do feriado de 20 de novembro a partir de 26-12-2023:
        >>> du.contar("20-11-2020", "21-11-2020")
        1
        >>> du.contar("20-11-2024", "21-11-2024")
        0

        Seleção explícita da lista anterior, que não inclui 20 de novembro:
        >>> du.contar("20-11-2024", "21-11-2024", calendario="anterior")
        1

        Contagem negativa quando ``inicio`` é posterior a ``fim``:
        >>> du.contar("08-01-2023", "01-01-2023")
        -5

        Total de dias úteis em janeiro e fevereiro desde o início do ano:
        >>> du.contar(inicio="01-01-2024", fim=["01-02-2024", "01-03-2024"]).to_list()
        [22, 41]

        Dias úteis restantes de janeiro/fevereiro até o fim do ano:
        >>> du.contar(["01-01-2024", "01-02-2024"], "01-01-2025").to_list()
        [253, 231]

        Total de dias úteis em janeiro e fevereiro de 2024:
        >>> inicios = ["01-01-2024", "01-02-2024"]
        >>> fins = ["01-02-2024", "01-03-2024"]
        >>> du.contar(inicios, fins).to_list()
        [22, 19]

        Valores nulos escalares são propagados:
        >>> du.contar(None, "01-01-2024") is None
        True
        >>> du.contar("01-01-2024", None) is None
        True

        Nulo dentro do array:
        >>> du.contar("01-01-2024", ["01-02-2024", None]).to_list()
        [22, None]

        >>> datas_inicio = ["01-01-2024", "01-02-2024", "01-03-2024"]
        >>> du.contar(datas_inicio, "01-01-2025").to_list()
        [253, 231, 212]
    """
    resultado = (
        pl.DataFrame(
            data={"inicio": inicio, "fim": fim},
            nan_to_null=True,
        )
        .select(dias_uteis=contar_expr("inicio", "fim", calendario))
        .get_column("dias_uteis")
    )

    if not tp.any_is_array_like(inicio, fim):
        return resultado.item()

    return resultado


def deslocar_expr(
    data: pl.Expr | str,
    deslocamento: int | pl.Expr | str,
    ajuste: Ajuste = "seguinte",
    calendario: Calendario = "auto",
) -> pl.Expr:
    """Cria uma expressão Polars para somar dias úteis.

    Ideal para operações vetorizadas em DataFrames ou LazyFrames.

    Args:
        data: Coluna de data original.
        deslocamento: Número de dias úteis a somar. Pode ser um inteiro fixo ou
            outra coluna.
        ajuste: Como tratar uma data inicial não-útil: ``"seguinte"`` avança
            para o próximo dia útil e ``"anterior"`` recua para o dia útil
            anterior. Padrão: ``"seguinte"``.
        calendario: Lista de feriados a considerar. ``"anterior"`` usa a lista
            vigente antes de 26-12-2023, ``"atual"`` usa a lista vigente a partir
            dessa data e ``"auto"`` seleciona a lista por linha com base em
            ``data``. Padrão: ``"auto"``.

    Returns:
        Uma ``pl.Expr`` que resulta em Date.

    Examples:
        >>> import datetime as dt
        >>> import polars as pl
        >>> from pyield import du
        >>> datas = [dt.date(2023, 12, 22), dt.date(2023, 12, 29)]
        >>> offsets = [1, 5]
        >>> df = pl.DataFrame({"dt": datas, "n": offsets})

        Adicionando um valor fixo (1 dia útil):
        >>> df.select(du.deslocar_expr("dt", 1).alias("t_plus_1"))
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
        >>> df.select(du.deslocar_expr("dt", "n").alias("vencimento"))
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

    data = cv.converter_datas_expr(data)

    return data.dt.add_business_days(
        n=deslocamento,
        roll=_traduzir_ajuste(ajuste),
        holidays=_expressao_feriados(data, calendario),
    )


@overload
def deslocar(
    datas: DatesLike,
    deslocamento: ArrayLike | int | None,
    ajuste: Ajuste = ...,
    calendario: Calendario = ...,
) -> pl.Series: ...
@overload
def deslocar(
    datas: DateLike | None,
    deslocamento: ArrayLike,
    ajuste: Ajuste = ...,
    calendario: Calendario = ...,
) -> pl.Series: ...
@overload
def deslocar(
    datas: DateLike,
    deslocamento: int,
    ajuste: Ajuste = ...,
    calendario: Calendario = ...,
) -> dt.date: ...
@overload
def deslocar(
    datas: None,
    deslocamento: int,
    ajuste: Ajuste = ...,
    calendario: Calendario = ...,
) -> None: ...
@overload
def deslocar(
    datas: DateLike,
    deslocamento: None,
    ajuste: Ajuste = ...,
    calendario: Calendario = ...,
) -> None: ...


def deslocar(
    datas: DateLike | DatesLike | None,
    deslocamento: int | ArrayLike | None,
    ajuste: Ajuste = "seguinte",
    calendario: Calendario = "auto",
) -> dt.date | pl.Series | None:
    """Desloca data(s) por um número de dias úteis com regime de feriados brasileiro.

    A operação é realizada em duas etapas por elemento:
    1) AJUSTE: Se a data original cair em fim de semana ou feriado, move-a de acordo
       com ``ajuste`` (``"seguinte"`` ou ``"anterior"``).
    2) DESLOCAMENTO: Aplica a quantidade de dias úteis com sinal (positivo avança,
       negativo retrocede, zero mantém a data após o ajuste).

    PRESERVAÇÃO DE ORDEM (crítico): A ordenação de saída corresponde estritamente
    ao pareamento elemento a elemento após expansão entre ``datas`` e ``deslocamento``.
    Nenhuma ordenação, deduplicação ou mudança de forma ocorre. O i-ésimo resultado
    corresponde ao i-ésimo par (data, deslocamento), permitindo atribuição segura
    de volta ao DataFrame de origem.

    Regime de feriados: Por padrão, para cada data, a lista de feriados apropriada
    (anterior vs. atual) é escolhida com base na data de transição 26-12-2023. Também
    é possível selecionar explicitamente uma das listas para todo o deslocamento.

    Semântica do ajuste: ``ajuste`` só atua quando a data original não é um dia útil
    sob seu regime. Após o ajuste, a adição de dias úteis subsequente é aplicada a
    partir dessa âncora. Um ``deslocamento`` de 0 retorna a data original, se ela já
    for útil, ou a data resultante do ajuste.

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
        datas: Data única ou coleção de datas a serem ajustadas, se necessário, e
            então deslocadas. Cada data seleciona independentemente o regime de
            feriados.
        deslocamento: Contagem com sinal de dias úteis a aplicar após o ajuste.
            Positivo move para frente, negativo para trás e zero mantém a data
            ajustada.
        ajuste: Como tratar uma data inicial não-útil: ``"seguinte"`` avança para
            o próximo dia útil e ``"anterior"`` recua para o dia útil anterior.
            Padrão: ``"seguinte"``.
        calendario: Lista de feriados a considerar. ``"anterior"`` usa a lista
            vigente antes de 26-12-2023, ``"atual"`` usa a lista vigente a partir
            dessa data e ``"auto"`` seleciona a lista por elemento com base em
            ``datas``. Padrão: ``"auto"``.

    Returns:
        Um ``date`` Python para entradas escalares, uma Series Polars de datas para
        qualquer entrada de array, ou ``None`` se um argumento escalar nulo foi
        fornecido.

    Notes:
        - Encapsulamento de ``polars.Expr.dt.add_business_days`` aplicado
          condicionalmente.
        - Com ``calendario="auto"``, o regime é decidido por elemento comparando
          com a data de transição 26-12-2023.
        - Fins de semana são sempre tratados como não-úteis.
        - Strings de data aceitas: ``DD-MM-YYYY``, ``DD/MM/YYYY`` e ``YYYY-MM-DD``.
        - Strings inválidas são tratadas como ``null`` e propagadas ao resultado.

    Examples:
        >>> from pyield import du

        Transição do feriado de 20 de novembro a partir de 26-12-2023:
        >>> du.deslocar("20-11-2020", 0)
        datetime.date(2020, 11, 20)
        >>> du.deslocar("20-11-2024", 0)
        datetime.date(2024, 11, 21)

        Seleção explícita da lista anterior, que não inclui 20 de novembro:
        >>> du.deslocar("20-11-2024", 0, calendario="anterior")
        datetime.date(2024, 11, 20)

        Desloca sábado antes do Natal para o próximo dia útil (terça após Natal):
        >>> du.deslocar("23-12-2023", 0)
        datetime.date(2023, 12, 26)

        Desloca sexta antes do Natal (sem deslocamento pois é dia útil):
        >>> du.deslocar("22-12-2023", 0)
        datetime.date(2023, 12, 22)

        Desloca para o dia útil anterior se não for útil (deslocamento=0 e
        ajuste="anterior"):

        Sem deslocamento pois é dia útil:
        >>> du.deslocar("22-12-2023", 0, ajuste="anterior")
        datetime.date(2023, 12, 22)

        Desloca para o primeiro dia útil antes de "23-12-2023":
        >>> du.deslocar("23-12-2023", 0, ajuste="anterior")
        datetime.date(2023, 12, 22)

        Avança para o próximo dia útil (deslocamento=1 e ajuste="seguinte"):

        Desloca sexta para o próximo dia útil (sexta é pulada -> segunda):
        >>> du.deslocar("27-09-2024", 1)
        datetime.date(2024, 9, 30)

        Desloca sábado para o próximo dia útil (segunda é pulada -> terça):
        >>> du.deslocar("28-09-2024", 1)
        datetime.date(2024, 10, 1)

        Volta para o dia útil anterior (deslocamento=-1 e ajuste="anterior"):

        Desloca sexta para o dia útil anterior (sexta é pulada -> quinta):
        >>> du.deslocar("27-09-2024", -1, ajuste="anterior")
        datetime.date(2024, 9, 26)

        Desloca sábado para o dia útil anterior (sexta é pulada -> quinta):
        >>> du.deslocar("28-09-2024", -1, ajuste="anterior")
        datetime.date(2024, 9, 26)

        Lista de datas e deslocamentos:
        >>> du.deslocar(["19-09-2024", "20-09-2024"], 1)
        shape: (2,)
        Series: 'data_ajustada' [date]
        [
            2024-09-20
            2024-09-23
        ]

        >>> du.deslocar("19-09-2024", [1, 2])  # lista de deslocamentos
        shape: (2,)
        Series: 'data_ajustada' [date]
        [
            2024-09-20
            2024-09-23
        ]

        Nulos escalares propagam para None:
        >>> du.deslocar(None, 1) is None
        True

        Nulo escalar propaga dentro de arrays:
        >>> du.deslocar(None, [1, 2])
        shape: (2,)
        Series: 'data_ajustada' [date]
        [
            null
            null
        ]

        Nulos dentro de arrays são preservados:
        >>> du.deslocar(["19-09-2024", None], 1)
        shape: (2,)
        Series: 'data_ajustada' [date]
        [
            2024-09-20
            null
        ]

        >>> datas = ["19-09-2024", "20-09-2024", "21-09-2024"]
        >>> du.deslocar(datas, 1)
        shape: (3,)
        Series: 'data_ajustada' [date]
        [
            2024-09-20
            2024-09-23
            2024-09-24
        ]
    """
    resultado = (
        pl.DataFrame(
            data={"datas": datas, "deslocamento": deslocamento},
            nan_to_null=True,
        )
        .select(
            data_ajustada=deslocar_expr(
                "datas",
                deslocamento="deslocamento",
                ajuste=ajuste,
                calendario=calendario,
            )
        )
        .get_column("data_ajustada")
    )

    if not tp.any_is_array_like(datas, deslocamento):
        return resultado.item()

    return resultado


def gerar(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    limites_inclusivos: LimitesInclusivos = "ambos",
    calendario: Calendario = "auto",
) -> pl.Series:
    """Gera uma Series de dias úteis entre ``inicio`` e ``fim``.

    Considera a lista de feriados brasileiros.

    Args:
        inicio: Data inicial. Se None, usa a data atual.
        fim: Data final. Se None, usa a data atual.
        limites_inclusivos: Define quais limites pertencem ao intervalo. Opções
            válidas: ``"ambos"``, ``"inicio"``, ``"fim"`` e ``"nenhum"``.
            Padrão: ``"ambos"``.
        calendario: Lista de feriados a considerar. ``"anterior"`` usa a lista
            vigente antes de 26-12-2023, ``"atual"`` usa a lista vigente a partir
            dessa data e ``"auto"`` seleciona a lista com base em ``inicio``.
            Padrão: ``"auto"``.

    Returns:
        Series de dias úteis (nome: 'data').

    Notes:
        - Strings de data aceitas: ``DD-MM-YYYY``, ``DD/MM/YYYY`` e ``YYYY-MM-DD``.
        - ``inicio`` e ``fim`` nulos usam a data atual.
        - Datas inválidas levantam ``ValueError``.

    Examples:
        >>> from pyield import du
        >>> du.gerar(inicio="22-12-2023", fim="02-01-2024")
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

        Seleção automática do calendário conforme a data inicial:
        >>> len(du.gerar("20-11-2020", "20-11-2020", calendario="auto"))
        1

        Inclusão apenas do limite inicial:
        >>> du.gerar(
        ...     "08-01-2024",
        ...     "10-01-2024",
        ...     limites_inclusivos="inicio",
        ... ).to_list()
        [datetime.date(2024, 1, 8), datetime.date(2024, 1, 9)]
    """
    hoje = relogio.hoje()
    inicio = cv.converter_datas(inicio) or hoje
    fim = cv.converter_datas(fim) or hoje
    feriados = _selecionar_feriados(inicio, calendario)

    datas = pl.date_range(
        inicio,
        fim,
        closed=_traduzir_limites_inclusivos(limites_inclusivos),
        eager=True,
    ).alias("data")
    return datas.filter(datas.dt.is_business_day(holidays=feriados))


def eh_dia_util_expr(
    data: pl.Expr | str,
    calendario: Calendario = "auto",
) -> pl.Expr:
    """Cria expressão Polars para verificar se é dia útil (True/False).

    Args:
        data: Coluna de datas ou expressão Polars.
        calendario: Lista de feriados a considerar. ``"anterior"`` usa a lista
            vigente antes de 26-12-2023, ``"atual"`` usa a lista vigente a partir
            dessa data e ``"auto"`` seleciona a lista por linha com base em
            ``data``. Padrão: ``"auto"``.

    Returns:
        Uma ``pl.Expr`` booleana.

    Examples:
        >>> import datetime as dt
        >>> import polars as pl
        >>> from pyield import du
        >>> datas = [dt.date(2023, 12, 25), dt.date(2023, 12, 26)]
        >>> df = pl.DataFrame({"data": datas})

        Criando uma flag booleana:
        >>> df.with_columns(eh_dia_util=du.eh_dia_util_expr("data"))
        shape: (2, 2)
        ┌────────────┬─────────────┐
        │ data       ┆ eh_dia_util │
        │ ---        ┆ ---         │
        │ date       ┆ bool        │
        ╞════════════╪═════════════╡
        │ 2023-12-25 ┆ false       │
        │ 2023-12-26 ┆ true        │
        └────────────┴─────────────┘

        Usando para filtrar apenas dias úteis:
        >>> df.filter(du.eh_dia_util_expr("data"))
        shape: (1, 1)
        ┌────────────┐
        │ data       │
        │ ---        │
        │ date       │
        ╞════════════╡
        │ 2023-12-26 │
        └────────────┘
    """
    data = cv.converter_datas_expr(data)

    return data.dt.is_business_day(holidays=_expressao_feriados(data, calendario))


@overload
def eh_dia_util(datas: None, calendario: Calendario = ...) -> None: ...
@overload
def eh_dia_util(datas: DateLike, calendario: Calendario = ...) -> bool: ...
@overload
def eh_dia_util(datas: DatesLike, calendario: Calendario = ...) -> pl.Series: ...


def eh_dia_util(
    datas: None | DateLike | DatesLike,
    calendario: Calendario = "auto",
) -> None | bool | pl.Series:
    """Determina se data(s) são dias úteis brasileiros.

    REGIME DE FERIADOS: Por padrão, para cada data, a lista de feriados apropriada
    (anterior vs. atual) é escolhida com base na data de transição 26-12-2023. Também
    é possível selecionar explicitamente uma das listas para toda a avaliação.

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
    ``'eh_dia_util'`` é produzida.

    FINS DE SEMANA: Sábados e domingos nunca são dias úteis independentemente do
    regime de feriados.

    Args:
        datas: Data única ou coleção (list/tuple/Polars Series).
            Pode incluir nulos que propagam. Entrada escalar nula retorna ``None``.
        calendario: Lista de feriados a considerar. ``"anterior"`` usa a lista
            vigente antes de 26-12-2023, ``"atual"`` usa a lista vigente a partir
            dessa data e ``"auto"`` seleciona a lista por elemento com base em
            ``datas``. Padrão: ``"auto"``.

    Returns:
        ``True`` se for dia útil, ``False`` caso contrário para entrada escalar;
        ``None`` para entrada escalar nula; ou uma Series Polars de booleanos
        (nome: ``'eh_dia_util'``) para entradas de array.

    Examples:
        >>> from pyield import du
        >>> du.eh_dia_util("25-12-2023")  # Natal (calendário anterior)
        False
        >>> du.eh_dia_util("20-11-2024")  # Dia Nacional de Zumbi
        False
        >>> du.eh_dia_util("20-11-2024", calendario="anterior")
        True

        Períodos mistos:
        >>> du.eh_dia_util(["22-12-2023", "26-12-2023"]).to_list()
        [True, True]

    Notes:
        - A data de transição é 26-12-2023.
        - Com ``calendario="auto"``, espelha a lógica por linha usada em
          ``contar`` e ``deslocar``.
        - Fins de semana sempre avaliam como ``False``.
        - Elementos nulos propagam.
        - Strings de data aceitas: ``DD-MM-YYYY``, ``DD/MM/YYYY`` e ``YYYY-MM-DD``.
        - Strings inválidas são tratadas como ``null`` e propagadas ao resultado.
    """
    resultado = (
        pl.DataFrame({"datas": datas}, nan_to_null=True)
        .select(eh_dia_util=eh_dia_util_expr("datas", calendario=calendario))
        .get_column("eh_dia_util")
    )

    if not tp.any_is_array_like(datas):
        return resultado.item()

    return resultado


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
    resultado = deslocar(hoje_brasil, 0, ajuste="anterior")
    assert isinstance(resultado, dt.date), (
        "Premissa violada: deslocar não retornou uma data para a data atual."
    )
    return resultado
