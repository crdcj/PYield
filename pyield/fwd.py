import datetime as dt
from typing import Sequence

import polars as pl

from pyield._internal.types import ArrayLike, any_is_empty


def forwards(
    bdays: ArrayLike,
    rates: ArrayLike,
    group_by: Sequence[str | int | dt.date] | pl.Series | None = None,
) -> pl.Series:
    r"""
    Calcula taxas a termo (forward rates) a partir de taxas zero (spot rates).

    A taxa a termo no vértice 'n' é definida como:
        fwdₖ = fwdⱼ→ₖ (a taxa a termo de j para k)

    A fórmula utilizada é:
        fwdₖ = ((1 + txₖ)^(duₖ/252) / (1 + txⱼ)^(duⱼ/252))^(252/(duₖ - duⱼ)) - 1

    Como au = du/252 (tempo em anos úteis), a fórmula pode ser simplificada para:

        fwdₖ = ((1 + txₖ)^auₖ / (1 + txⱼ)^auⱼ)^(1/(auₖ - auⱼ)) - 1


    Em LaTeX, a fórmula é representada como:

    $$
    fwd_k = \left( \frac{(1 + tx_k)^{au_k}}{(1 + tx_j)^{au_j}} \right)^{\frac{1}{au_k - au_j}} - 1
    $$

    Onde:
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
    a termo para diferentes grupos de datas. O agrupamento é feito com base em `group_by`.
    Se este argumento for None, todos os dados serão tratados como um único grupo.

    A função calcula as taxas a termo para todos os pontos, exceto o primeiro
    de cada grupo, que é tratado separadamente.

    Args:
        bdays (ArrayLike): Número de dias úteis (du) para cada taxa zero.
        rates (ArrayLike): Taxas zero (tx) correspondentes aos dias úteis.
        group_by (Sequence[str | int | date] | pl.Series | None, optional):
            Critério de agrupamento para os cálculos (ex: datas de referência,
            tickers de títulos). Pode ser uma lista/série de strings, inteiros
            ou datas. Se None, todos os dados são tratados como um único grupo.
            Default None.

    Returns:
        pl.Series: Série contendo as taxas a termo calculadas (tipo Float64).
            A primeira taxa de cada grupo corresponde à taxa zero inicial.

    Raises:
        polars.exceptions.ShapeError: Se os comprimentos de `bdays`, `rates`
            e `group_by` (quando fornecido) não forem iguais.

    Examples:
        >>> bdays = [10, 20, 30]
        >>> rates = [0.05, 0.06, 0.07]
        >>> yd.forwards(bdays, rates)
        shape: (3,)
        Series: 'fwd' [f64]
        [
            0.05
            0.070095
            0.090284
        ]

        >>> # Exemplo com agrupamento (a última está isolada em outro grupo)
        >>> group_by = ["LTN", "LTN", "NTN-F"]
        >>> yd.forwards(bdays, rates, group_by)
        shape: (3,)
        Series: 'fwd' [f64]
        [
            0.05
            0.070095
            0.07
        ]

        >>> # Exemplo com taxas indicativas de NTN-B em 16-09-2025
        >>> from pyield import ntnb
        >>> df = ntnb.data("16-09-2025")
        >>> yd.forwards(df["BDToMat"], df["IndicativeRate"])
        shape: (13,)
        Series: 'fwd' [f64]
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
        Series: 'fwd' [f64]
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
        Series: 'fwd' [f64]
        [
            0.0943
            0.072439
            0.071549
            0.0943
        ]

    Notes:
        - A função ordena os dados de entrada primeiro por `group_by`,
        se for fornecido, e depois por `bdays` para garantir a ordem cronológica
        correta no cálculo das taxas a termo.
        - Valores nulos em `bdays` ou `rates` são ignorados no cálculo,
        resultando em valores nulos nas posições correspondentes na saída.
        - Os resultados são retornados na mesma ordem dos dados de entrada.
    """  # noqa: E501
    # Validações iniciais
    if any_is_empty(bdays, rates):
        return pl.Series(dtype=pl.Float64)

    # 1. Montar o DataFrame
    df_orig = pl.DataFrame(
        {
            "du_k": bdays,
            "tx_k": rates,
            "group_by": 0 if group_by is None else group_by,
        }
    )

    # 2. Definir a fórmula da taxa a termo
    # fₖ = fⱼ→ₖ = ((1 + txₖ)^auₖ / (1 + txⱼ)^auⱼ) ^ (1/(auₖ - auⱼ)) - 1
    fator_k = (1 + pl.col("tx_k")) ** pl.col("au_k")  # (1 + txₖ)^auₖ
    fator_j = (1 + pl.col("tx_j")) ** pl.col("au_j")  # (1 + txⱼ)^auⱼ
    expoente = 1 / (pl.col("au_k") - pl.col("au_j"))  # 1/(auₖ - auⱼ)
    fwd_exp = (fator_k / fator_j) ** expoente - 1

    # 3. Calcular as taxas a termo
    df_fwd = (
        df_orig.drop_nans()
        .drop_nulls()
        .unique(subset=["du_k", "group_by"], keep="last")
        .sort("group_by", "du_k")
        .with_columns(au_k=pl.col("du_k") / 252)  # Criar coluna de anos úteis
        .with_columns(
            # Calcular os valores deslocados (shift) dentro de cada grupo
            tx_j=pl.col("tx_k").shift(1).over("group_by"),
            au_j=pl.col("au_k").shift(1).over("group_by"),
        )
        .with_columns(fwd=fwd_exp)
        .with_columns(
            # A matriz de cálculo já foi tratada: ela está deduplicada,
            # sem nulos e ordenada por group_by e du_k. Então, basta
            # ajustar a primeira taxa fwd de cada grupo para ser igual à taxa spot!
            fwd=pl.when(pl.col("du_k") == pl.first("du_k").over("group_by"))
            .then("tx_k")
            .otherwise("fwd")
        )
    )
    # 4. Reunir os resultados na ordem original
    df_orig = df_orig.join(
        df_fwd.drop("tx_k"),  # tx_k já existe em df_orig
        on=["du_k", "group_by"],
        how="left",
        maintain_order="left",
    )

    # Retornar a série de taxas a termo
    return df_orig["fwd"]


def forward(
    bday1: int,
    bday2: int,
    rate1: float,
    rate2: float,
) -> float:
    r"""
    Calcula a taxa a termo (forward rate) entre dois prazos (dias úteis).

    Utiliza a fórmula:
        f₁→₂ = ((1 + tx₂)^(du₂/252) / (1 + tx₁)^(du₁/252))^(252/(du₂ - du₁)) - 1

    Onde:
        - tx₁ é a taxa zero para o primeiro prazo (du₁).
        - tx₂ é a taxa zero para o segundo prazo (du₂).
        - du₁ é o número de dias úteis até a primeira data.
        - du₂ é o número de dias úteis até a segunda data.
        - A constante 252 representa o número de dias úteis no ano.

    Como au = du/252 (tempo em anos úteis), a fórmula pode ser simplificada para:

        f₁→₂ = ((1 + tx₂)^au₂ / (1 + tx₁)^au₁)^(1/(au₂ - au₁)) - 1

    Que em latex fica:
    $$
    f_{1 \rightarrow 2} = \left( \frac{(1 + tx_2)^{au_2}}{(1 + tx_1)^{au_1}} \right)^{\frac{1}{au_2 - au_1}} - 1
    $$

    Args:
        bday1 (int): Número de dias úteis do primeiro ponto (prazo menor).
        bday2 (int): Número de dias úteis do segundo ponto (prazo maior).
        rate1 (float): Taxa zero (spot rate) para o prazo `bday1`.
        rate2 (float): Taxa zero (spot rate) para o prazo `bday2`.

    Returns:
        float: A taxa a termo calculada entre `bday1` e `bday2`. Retorna
            `nan` se `bday1 >= bday2` ou se qualquer um dos
            argumentos de entrada for float('nan') ou None.

    Examples:
        >>> # Exemplo válido: bday2 > bday1
        >>> yd.forward(10, 20, 0.05, 0.06)
        0.0700952380952371
        >>> # Exemplo inválido: bday1 >= bday2
        >>> print(yd.forward(20, 10, 0.06, 0.05))
        nan

        >>> # Argumentos nulos retornam nan
        >>> print(yd.forward(10, 20, 0.05, None))
        nan

    Notes:
        `bday2` precisa ser necessariamente maior que `bday1` para que
        o cálculo da taxa a termo seja matematicamente válido.
    """  # noqa: E501
    if any_is_empty(rate1, rate2, bday1, bday2):
        # Se qualquer entrada for nula/NaN, retorna NaN
        return float("nan")

    # Prazo final deve ser maior que o inicial
    if bday2 <= bday1:
        return float("nan")

    # Converter dias úteis para anos úteis
    au1 = bday1 / 252
    au2 = bday2 / 252

    # f₁→₂ = ((1 + tx₂)^au₂ / (1 + tx₁)^au₁)^(1/(au₂ - au₁)) - 1
    return ((1 + rate2) ** au2 / (1 + rate1) ** au1) ** (1 / (au2 - au1)) - 1
