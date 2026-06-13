import datetime as dt
from typing import Sequence

import polars as pl

from pyield._internal.types import ArrayLike, any_is_empty


def forwards_expr(
    dias_uteis: pl.Expr | str,
    taxas: pl.Expr | str,
    agrupar_por: pl.Expr | str | None = None,
) -> pl.Expr:
    r"""
    Cria uma expressão Polars para calcular taxas a termo dentro de DataFrames.

    Versão expressiva de :func:`forwards`, no mesmo estilo de ``du.contar_expr``.
    Pensada para uso dentro de ``with_columns()`` ou ``select()``, especialmente
    em DataFrames com múltiplas datas de referência (ex.: séries históricas de
    DI1), onde ``agrupar_por`` define janelas independentes de cálculo sem que
    seja necessário extrair colunas, calcular fora e juntar de volta.

    A fórmula da taxa a termo entre os vértices ``j`` (anterior) e ``k``
    (atual) é:

    \[
    fwd_k = \left( \frac{f_k^{au_k}}{f_j^{au_j}} \right)^{\frac{1}{au_k - au_j}} - 1
    \]

    Onde ``fₓ = 1 + txₓ`` e ``auₓ = duₓ/252``. A primeira linha de cada grupo
    (menor ``dias_uteis``) é tratada como spot: ``fwd = tx``.

    Ordenação cronológica:
        A expressão usa ``shift(1).over(agrupar_por, order_by=dias_uteis)``,
        calculando a taxa do vértice anterior em ordem de ``dias_uteis`` dentro
        de cada grupo **sem reordenar o DataFrame** de origem.

    Propagação de nulos e NaN:
        Se ``dias_uteis`` ou ``taxas`` for nulo em uma linha, o resultado é
        nulo nessa linha; se for NaN, o resultado é NaN (nulos e NaN não se
        misturam). Em ambos os casos, a linha imediatamente posterior em
        ordem de ``dias_uteis`` dentro do mesmo grupo também fica nula/NaN,
        pois seu vértice anterior é inválido. Como nulos são ordenados ao
        fim em ``order_by=dias_uteis``, um null em ``dias_uteis`` afeta
        apenas a própria linha. Mesmo contrato de :func:`forwards`.

    Duplicatas em ``(agrupar_por, dias_uteis)``:
        Duplicatas tornam o vértice ambíguo. A função invalida a taxa com
        nulo nas próprias linhas duplicadas; a partir daí o cascateamento
        natural de nulos vale (a linha imediatamente posterior em ordem de
        ``dias_uteis`` dentro do grupo também fica nula; a seguinte volta
        ao normal). Mesmo contrato de :func:`forwards`.

    Args:
        dias_uteis (pl.Expr | str): Nome de coluna ou expressão Polars com
            o prazo em dias úteis para cada taxa zero.
        taxas (pl.Expr | str): Nome de coluna ou expressão Polars com a taxa
            zero correspondente a cada ``dias_uteis``.
        agrupar_por (pl.Expr | str | None, optional): Nome de coluna,
            expressão ou ``None``. Define janelas independentes de cálculo
            (ex.: ``"data_referencia"``, ``"ticker"``). Quando ``None``, todas
            as linhas formam um único grupo. Padrão ``None``.

    Returns:
        pl.Expr: Expressão Float64 com a taxa a termo de cada linha. A
        expressão não recebe alias; nomeie no momento do uso, por exemplo
        via ``with_columns(taxa_forward=forwards_expr(...))``.

    Examples:
        >>> df = pl.DataFrame(
        ...     {
        ...         "data_referencia": ["2025-01-02"] * 3 + ["2025-01-03"] * 3,
        ...         "dias_uteis": [10, 20, 30, 10, 20, 30],
        ...         "taxa": [0.05, 0.06, 0.07, 0.06, 0.07, 0.08],
        ...     }
        ... )
        >>> df.with_columns(
        ...     taxa_forward=yd.forwards_expr(
        ...         "dias_uteis", "taxa", agrupar_por="data_referencia"
        ...     )
        ... )
        shape: (6, 4)
        ┌─────────────────┬────────────┬──────┬──────────────┐
        │ data_referencia ┆ dias_uteis ┆ taxa ┆ taxa_forward │
        │ ---             ┆ ---        ┆ ---  ┆ ---          │
        │ str             ┆ i64        ┆ f64  ┆ f64          │
        ╞═════════════════╪════════════╪══════╪══════════════╡
        │ 2025-01-02      ┆ 10         ┆ 0.05 ┆ 0.05         │
        │ 2025-01-02      ┆ 20         ┆ 0.06 ┆ 0.070095     │
        │ 2025-01-02      ┆ 30         ┆ 0.07 ┆ 0.090284     │
        │ 2025-01-03      ┆ 10         ┆ 0.06 ┆ 0.06         │
        │ 2025-01-03      ┆ 20         ┆ 0.07 ┆ 0.080094     │
        │ 2025-01-03      ┆ 30         ┆ 0.08 ┆ 0.100281     │
        └─────────────────┴────────────┴──────┴──────────────┘

        Sem agrupamento, todas as linhas formam um único grupo:

        >>> df = pl.DataFrame({"du": [10, 20, 30], "tx": [0.05, 0.06, 0.07]})
        >>> df.with_columns(fwd=yd.forwards_expr("du", "tx"))
        shape: (3, 3)
        ┌─────┬──────┬──────────┐
        │ du  ┆ tx   ┆ fwd      │
        │ --- ┆ ---  ┆ ---      │
        │ i64 ┆ f64  ┆ f64      │
        ╞═════╪══════╪══════════╡
        │ 10  ┆ 0.05 ┆ 0.05     │
        │ 20  ┆ 0.06 ┆ 0.070095 │
        │ 30  ┆ 0.07 ┆ 0.090284 │
        └─────┴──────┴──────────┘

        Duplicatas em ``(grupo, du)`` invalidam o vértice. Aqui o segundo
        grupo (``2025-01-03``) tem duplicata em ``du=20``: ambas as linhas
        com ``du=20`` ficam nulas e a linha ``du=30`` também (cascata local);
        o primeiro grupo permanece intacto.

        >>> df = pl.DataFrame(
        ...     {
        ...         "dr": ["2025-01-02"] * 3 + ["2025-01-03"] * 4,
        ...         "du": [10, 20, 30, 10, 20, 20, 30],
        ...         "tx": [0.05, 0.06, 0.07, 0.05, 0.06, 0.061, 0.07],
        ...     }
        ... )
        >>> df.with_columns(fwd=yd.forwards_expr("du", "tx", agrupar_por="dr"))
        shape: (7, 4)
        ┌────────────┬─────┬───────┬──────────┐
        │ dr         ┆ du  ┆ tx    ┆ fwd      │
        │ ---        ┆ --- ┆ ---   ┆ ---      │
        │ str        ┆ i64 ┆ f64   ┆ f64      │
        ╞════════════╪═════╪═══════╪══════════╡
        │ 2025-01-02 ┆ 10  ┆ 0.05  ┆ 0.05     │
        │ 2025-01-02 ┆ 20  ┆ 0.06  ┆ 0.070095 │
        │ 2025-01-02 ┆ 30  ┆ 0.07  ┆ 0.090284 │
        │ 2025-01-03 ┆ 10  ┆ 0.05  ┆ 0.05     │
        │ 2025-01-03 ┆ 20  ┆ 0.06  ┆ null     │
        │ 2025-01-03 ┆ 20  ┆ 0.061 ┆ null     │
        │ 2025-01-03 ┆ 30  ┆ 0.07  ┆ null     │
        └────────────┴─────┴───────┴──────────┘

    Notes:
        - A ordem original do DataFrame é preservada.
        - Para uso fora de DataFrames (arrays, Series soltas), use
          :func:`forwards`.
    """
    du_k = pl.col(dias_uteis) if isinstance(dias_uteis, str) else dias_uteis
    tx_k = pl.col(taxas) if isinstance(taxas, str) else taxas

    if agrupar_por is None:
        grupo: pl.Expr = pl.lit(0)
    elif isinstance(agrupar_por, str):
        grupo = pl.col(agrupar_por)
    else:
        grupo = agrupar_por

    # Duplicatas em (grupo, du_k) tornam o vértice ambíguo. Invalidamos a
    # taxa com null nas linhas duplicadas e deixamos o cascateamento natural
    # de nulos cuidar do resto (mesma semântica de um tx_k null).
    eh_duplicada = pl.len().over(grupo, du_k) > 1
    tx_k = pl.when(eh_duplicada).then(None).otherwise(tx_k)

    au_k = du_k / 252
    tx_j = tx_k.shift(1).over(grupo, order_by=du_k)
    au_j = au_k.shift(1).over(grupo, order_by=du_k)

    # fwdₖ = (fₖ^auₖ / fⱼ^auⱼ) ^ (1/(auₖ - auⱼ)) - 1, com fₓ = 1 + txₓ
    fk = 1 + tx_k
    fj = 1 + tx_j
    taxa_forward = (fk**au_k / fj**au_j) ** (1 / (au_k - au_j)) - 1

    # Primeira linha de cada grupo (menor dias_uteis) é a taxa spot
    eh_primeira = du_k == du_k.min().over(grupo)
    return pl.when(eh_primeira).then(tx_k).otherwise(taxa_forward)


def forwards(
    dias_uteis: ArrayLike,
    taxas: ArrayLike,
    agrupar_por: Sequence[str | int | dt.date] | pl.Series | None = None,
) -> pl.Series:
    r"""
    Calcula taxas a termo a partir de taxas zero.

    A taxa a termo no vértice 'n' é definida como:

        fwdₖ = fwdⱼ→ₖ (a taxa a termo de j para k)

    Definindo o fator de capitalização no vértice k como:

        fₖ = 1 + txₖ

    A fórmula utilizada é:

        fwdₖ = (fₖ^(duₖ/252) / fⱼ^(duⱼ/252))^(252/(duₖ - duⱼ)) - 1

    Como au = du/252 (tempo em anos úteis), a fórmula pode ser simplificada para:

        fwdₖ = (fₖ^auₖ / fⱼ^auⱼ)^(1/(auₖ - auⱼ)) - 1

    Em LaTeX, a fórmula é representada como:

    \[
    fwd_k = \left( \frac{f_k^{au_k}}{f_j^{au_j}} \right)^{\frac{1}{au_k - au_j}} - 1
    \]

    Onde:
    - fⱼ é o fator de capitalização no vértice anterior (fⱼ = 1 + txⱼ).
    - fₖ é o fator de capitalização no vértice atual (fₖ = 1 + txₖ).
    - txⱼ é a taxa zero para o vértice anterior.
    - txₖ é a taxa zero para o vértice atual.
    - auⱼ é o prazo em anos úteis no vértice anterior (auⱼ = duⱼ/252).
    - auₖ é o prazo em anos úteis no vértice atual (auₖ = duₖ/252).
    - A constante 252 representa o número de dias úteis no ano.

    A função preserva a ordem original dos dados de entrada. Nulos em
    ``dias_uteis`` ou ``taxas`` produzem nulo na linha correspondente; NaN
    produz NaN (nulos e NaN não se misturam). Duplicatas em ``(agrupar_por,
    dias_uteis)`` tornam o vértice ambíguo e produzem nulo nas linhas
    duplicadas. Em todos esses casos a linha imediatamente posterior em ordem
    de ``dias_uteis`` dentro do mesmo grupo também fica inválida (nula ou
    NaN), pois seu vértice anterior é inválido; a seguinte volta ao normal.
    Este contrato é alinhado com :func:`forwards_expr`.

    A primeira taxa a termo de cada grupo é definida como a
    taxa zero desse primeiro vértice (fwd₁ = tx₁), dado que não existe um vértice
    anterior a tx₁ para se calcular a taxa a termo no primeiro ponto.

    A função também lida com agrupamentos opcionais, permitindo calcular taxas
    a termo para diferentes grupos de dados. O agrupamento é feito com base em
    `agrupar_por`. Se este argumento for None, todos os dados serão tratados
    como um único grupo.

    A função calcula as taxas a termo para todos os pontos, exceto o primeiro
    de cada grupo, que é tratado separadamente.

    Args:
        dias_uteis (ArrayLike): Número de dias úteis (du) para cada taxa zero.
        taxas (ArrayLike): Taxas zero (tx) correspondentes aos dias úteis.
        agrupar_por (Sequence[str | int | date] | pl.Series | None, optional):
            Critério de agrupamento para os cálculos (ex: datas de referência,
            tickers de títulos). Pode ser uma lista/série de strings, inteiros
            ou datas. Se None, todos os dados são tratados como um único grupo.
            Padrão None.

    Returns:
        pl.Series: Série contendo as taxas a termo calculadas (tipo Float64).
            A primeira taxa de cada grupo corresponde à taxa zero inicial.

    Raises:
        polars.exceptions.ShapeError: Se os comprimentos de `dias_uteis`,
            `taxas` e `agrupar_por` (quando fornecido) não forem iguais.

    Examples:
        >>> dias_uteis = [10, 20, 30]
        >>> taxas = [0.05, 0.06, 0.07]
        >>> yd.forwards(dias_uteis, taxas)
        shape: (3,)
        Series: 'taxa_forward' [f64]
        [
            0.05
            0.070095
            0.090284
        ]

        >>> # Exemplo com agrupamento (a última está isolada em outro grupo)
        >>> agrupar_por = ["LTN", "LTN", "NTN-F"]
        >>> yd.forwards(dias_uteis, taxas, agrupar_por)
        shape: (3,)
        Series: 'taxa_forward' [f64]
        [
            0.05
            0.070095
            0.07
        ]

        >>> # Exemplo com taxas indicativas de NTN-B em 16-09-2025
        >>> from pyield import ntnb
        >>> df = ntnb.dados("16-09-2025")
        >>> yd.forwards(df["dias_uteis"], df["taxa_indicativa"])
        shape: (13,)
        Series: 'taxa_forward' [f64]
        [
            0.0943
            0.071549
            0.072439
            0.069558
            0.076614
            …
            0.068105
            0.071278
            0.069117
            0.070373
            0.073286
        ]

        >>> # Valores nulos em ``dias_uteis`` produzem nulo na própria linha;
        >>> # como nulos vão para o fim em ordem de ``dias_uteis``, a linha
        >>> # com du=914 vê 730 como vértice anterior e calcula normalmente.
        >>> du = [230, 415, 730, None, 914]
        >>> tx = [0.0943, 0.084099, 0.079052, 0.1, 0.077134]
        >>> yd.forwards(du, tx)
        shape: (5,)
        Series: 'taxa_forward' [f64]
        [
            0.0943
            0.071549
            0.072439
            null
            0.069558
        ]

        >>> # Já um nulo em ``taxas`` propaga em cascata: a própria linha fica
        >>> # nula e a próxima em ordem de ``dias_uteis`` também, pois seu
        >>> # vértice anterior é nulo. A linha seguinte volta ao normal.
        >>> du = [230, 415, 730, 914]
        >>> tx = [0.0943, None, 0.079052, 0.077134]
        >>> yd.forwards(du, tx)
        shape: (4,)
        Series: 'taxa_forward' [f64]
        [
            0.0943
            null
            null
            0.069558
        ]

        >>> # NaN se comporta como nulo, mas mantém o dtype NaN no resultado
        >>> # (nulos e NaN não se misturam).
        >>> du = [230, 415, 730, 914]
        >>> tx = [0.0943, float("nan"), 0.079052, 0.077134]
        >>> yd.forwards(du, tx)
        shape: (4,)
        Series: 'taxa_forward' [f64]
        [
            0.0943
            NaN
            NaN
            0.069558
        ]

        >>> # Duplicatas em ``dias_uteis`` dentro de um grupo tornam o
        >>> # vértice ambíguo: ambas as linhas duplicadas ficam nulas e a
        >>> # linha imediatamente posterior em ordem de ``dias_uteis`` também
        >>> # (cascata local de nulos). A linha seguinte volta ao normal.
        >>> du = [230, 730, 415, 230]
        >>> tx = [0.1, 0.079052, 0.084099, 0.0943]
        >>> yd.forwards(du, tx)
        shape: (4,)
        Series: 'taxa_forward' [f64]
        [
            null
            0.072439
            null
            null
        ]

    Notes:
        - A função ordena os dados de entrada primeiro por `agrupar_por`,
          se for fornecido, e depois por `dias_uteis` para garantir a ordem
          cronológica correta no cálculo das taxas a termo.
        - Nulos em `dias_uteis` ou `taxas` produzem nulo na linha
          correspondente; NaN produz NaN. Duplicatas em `(agrupar_por,
          dias_uteis)` tornam o vértice ambíguo e produzem nulo nas linhas
          duplicadas. Em todos esses casos, a linha imediatamente posterior
          em ordem de `dias_uteis` dentro do mesmo grupo também fica
          inválida; a seguinte volta ao normal. Mesmo contrato de
          :func:`forwards_expr`.
        - Os resultados são retornados na mesma ordem dos dados de entrada.
    """
    # Validações iniciais
    if any_is_empty(dias_uteis, taxas):
        return pl.Series(dtype=pl.Float64)

    # Delega para a primitiva expressiva. A expressão preserva a ordem
    # original do DataFrame e cuida de nulos, NaN e duplicatas em (du, grupo)
    # via propagação local de nulos (ver Notes).
    df = pl.DataFrame(
        {
            "du_k": dias_uteis,
            "tx_k": taxas,
            "grupo": 0 if agrupar_por is None else agrupar_por,
        }
    )
    return df.with_columns(taxa_forward=forwards_expr("du_k", "tx_k", "grupo"))[
        "taxa_forward"
    ]


def forward(
    du1: int,
    du2: int,
    taxa1: float,
    taxa2: float,
) -> float:
    r"""
    Calcula a taxa a termo entre dois prazos (dias úteis).

    Utiliza a fórmula:

        f₁→₂ = (f₂^(du₂/252) / f₁^(du₁/252))^(252/(du₂ - du₁)) - 1

    Onde:
        - f₁ é o fator de capitalização do primeiro prazo (f₁ = 1 + tx₁).
        - f₂ é o fator de capitalização do segundo prazo (f₂ = 1 + tx₂).
        - tx₁ é a taxa zero para o primeiro prazo (du₁).
        - tx₂ é a taxa zero para o segundo prazo (du₂).
        - du₁ é o número de dias úteis até a primeira data.
        - du₂ é o número de dias úteis até a segunda data.
        - A constante 252 representa o número de dias úteis no ano.

    Como au = du/252 (tempo em anos úteis), a fórmula pode ser simplificada para:

        f₁→₂ = (f₂^au₂ / f₁^au₁)^(1/(au₂ - au₁)) - 1

    Que em latex fica:

    \[
    f_{1 \rightarrow 2} = \left( \frac{f_2^{au_2}}{f_1^{au_1}} \right)^{\frac{1}{au_2 - au_1}} - 1
    \]

    Args:
        du1 (int): Número de dias úteis do primeiro ponto (prazo menor).
        du2 (int): Número de dias úteis do segundo ponto (prazo maior).
        taxa1 (float): Taxa zero para o prazo `du1`.
        taxa2 (float): Taxa zero para o prazo `du2`.

    Returns:
        float: A taxa a termo calculada entre `du₁` e `du₂`. Retorna
            `nan` se `du₁ >= du₂` ou se qualquer um dos
            argumentos de entrada for `float("nan")` ou `None`.

    Examples:
        >>> # Exemplo válido: du₂ > du₁
        >>> yd.forward(10, 20, 0.05, 0.06)
        0.0700952380952371
        >>> # Exemplo inválido: du₁ >= du₂
        >>> print(yd.forward(20, 10, 0.06, 0.05))
        nan

    Notes:
        `du₂` precisa ser necessariamente maior que `du₁` para que
        o cálculo da taxa a termo seja matematicamente válido.
    """
    if any_is_empty(taxa1, taxa2, du1, du2):
        # Se qualquer entrada for nula/NaN, retorna NaN
        return float("nan")

    # Prazo final deve ser maior que o inicial
    if du2 <= du1:
        return float("nan")

    # Converter dias úteis para anos úteis
    au1 = du1 / 252
    au2 = du2 / 252

    # Definição dos fatores de capitalização:
    # f₁ = 1 + tx₁ e f₂ = 1 + tx₂
    f1 = 1 + taxa1
    f2 = 1 + taxa2

    # f₁→₂ = (f₂^au₂ / f₁^au₁)^(1/(au₂ - au₁)) - 1
    return (f2**au2 / f1**au1) ** (1 / (au2 - au1)) - 1
