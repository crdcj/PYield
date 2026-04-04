import datetime as dt
from typing import Sequence

import polars as pl

from pyield._internal.types import ArrayLike, any_is_empty


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

    A função preserva a ordem original dos dados de entrada e lida com valores nulos
    de forma apropriada. Valores nulos nas entradas resultarão em valores nulos
    nas taxas a termo calculadas.

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

        >>> # Valores nulos são descartados no cálculo e retornados como nulos
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

        >>> # O algoritmo ordena os dados de entrada antes do cálculo e retorna
        >>> # os resultados na ordem original. Valores duplicados são tratados
        >>> # como um único ponto no cálculo da taxa a termo (último valor é mantido).
        >>> du = [230, 730, 415, 230]
        >>> tx = [0.1, 0.079052, 0.084099, 0.0943]
        >>> yd.forwards(du, tx)
        shape: (4,)
        Series: 'taxa_forward' [f64]
        [
            0.0943
            0.072439
            0.071549
            0.0943
        ]

    Notes:
        - A função ordena os dados de entrada primeiro por `agrupar_por`,
          se for fornecido, e depois por `dias_uteis` para garantir a ordem
          cronológica correta no cálculo das taxas a termo.
        - Valores nulos em `dias_uteis` ou `taxas` são ignorados no cálculo,
          resultando em valores nulos nas posições correspondentes na saída.
        - Os resultados são retornados na mesma ordem dos dados de entrada.
    """
    # Validações iniciais
    if any_is_empty(dias_uteis, taxas):
        return pl.Series(dtype=pl.Float64)

    # 1. Montar o DataFrame
    df_orig = pl.DataFrame(
        {
            "du_k": dias_uteis,
            "tx_k": taxas,
            "grupo": 0 if agrupar_por is None else agrupar_por,
        }
    )

    # 2. Definir a fórmula da taxa a termo
    # Definição dos fatores de capitalização:
    # fₖ = 1 + txₖ e fⱼ = 1 + txⱼ
    fk = 1 + pl.col("tx_k")
    fj = 1 + pl.col("tx_j")
    # fwdₖ  = fwdⱼ→ₖ = (fₖ^auₖ / fⱼ^auⱼ) ^ (1/(auₖ - auⱼ)) - 1
    fator_k = fk ** pl.col("au_k")
    fator_j = fj ** pl.col("au_j")
    expoente = 1 / (pl.col("au_k") - pl.col("au_j"))  # 1/(auₖ - auⱼ)
    taxa_forward = (fator_k / fator_j) ** expoente - 1

    # 3. Calcular as taxas a termo
    df_fwd = (
        df_orig.drop_nans()
        .drop_nulls()
        .unique(subset=["du_k", "grupo"], keep="last")
        .sort("grupo", "du_k")
        .with_columns(au_k=pl.col("du_k") / 252)  # Criar coluna de anos úteis
        .with_columns(
            # Calcular os valores deslocados (shift) dentro de cada grupo
            tx_j=pl.col("tx_k").shift(1).over("grupo"),
            au_j=pl.col("au_k").shift(1).over("grupo"),
        )
        .with_columns(taxa_forward=taxa_forward)
        .with_columns(
            # A matriz de cálculo já foi tratada: ela está deduplicada,
            # sem nulos e ordenada por grupo e du_k. Então, basta
            # ajustar a primeira taxa forward de cada grupo para ser igual à taxa spot!
            taxa_forward=pl.when(pl.col("du_k") == pl.first("du_k").over("grupo"))
            .then("tx_k")
            .otherwise("taxa_forward")
        )
    )
    # 4. Reunir os resultados na ordem original
    df_orig = df_orig.join(
        df_fwd.drop("tx_k"),  # tx_k já existe em df_orig
        on=["du_k", "grupo"],
        how="left",
        maintain_order="left",
    )

    # Retornar a série de taxas a termo
    return df_orig["taxa_forward"]


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
