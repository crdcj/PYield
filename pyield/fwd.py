import pandas as pd
import polars as pl


def forwards(
    bdays: pd.Series | pl.Series | list[int] | tuple[int],
    rates: pd.Series | pl.Series | list[float] | tuple[float],
    groupby_dates: pd.Series | None = None,
) -> pd.Series:
    r"""
    Calcula taxas a termo (forward rates) a partir de taxas zero (spot rates).

    A taxa a termo no vértice 'n' é definida como:
        fwdₖ = fwdⱼ→ₖ (a taxa a termo de j para k)

    A fórmula utilizada é:
        fwdₖ = ((1 + rₖ)^(duₖ/252) / (1 + rⱼ)^(duⱼ/252))^(252/(duₖ - duⱼ)) - 1

    Como du/252 = t (tempo em anos úteis), a fórmula pode ser simplificada para:

        fwdₖ = ((1 + rₖ)^tₖ / (1 + rⱼ)^tⱼ)^(1/(tₖ - tⱼ)) - 1

    Em LaTeX, a fórmula é representada como:
    $$
    fwd_k = \left( \frac{(1 + r_k)^{t_k}}{(1 + r_j)^{t_j}} \right)^{\frac{1}{t_k - t_j}} - 1
    $$

    Onde:
        - rⱼ é a taxa zero para o vértice anterior.
        - rₖ é a taxa zero para o vértice atual.
        - tⱼ é o prazo em anos para o vértice anterior (calculado como duⱼ/252).
        - tₖ é o prazo em anos para o vértice atual (calculado como duₖ/252).
        - A constante 252 representa o número de dias úteis no ano.

    A primeira taxa a termo de cada grupo é definida como a
    taxa zero desse primeiro vértice (fwd₁ = r₁), dado que não existe um vértice
    anterior a r₁ para se calcular a taxa a termo no primeiro ponto.

    Valores nulos nas taxas ou prazos de entrada resultarão em valores nulos
    nas taxas a termo calculadas. A função também lida com agrupamentos
    opcionais, permitindo calcular taxas a termo para diferentes grupos de
    datas. O agrupamento é feito com base na coluna `groupby_dates`, que
    deve ser fornecida como uma série de pandas. Se `groupby_dates` for None,
    todos os dados serão tratados como um único grupo.
    A função calcula as taxas a termo para todos os pontos, exceto o primeiro
    de cada grupo, que é tratado separadamente.

    Args:
        bdays (pd.Series): Número de dias úteis para cada taxa zero.
        rates (pd.Series): Taxas zero correspondentes aos dias úteis.
        groupby_dates (pd.Series | None, optional): Critério de agrupamento
            opcional para segmentar os cálculos (ex: por data de referência).
            Se None, todos os dados são tratados como um único grupo.
            Default None.

    Returns:
        pd.Series: Série contendo as taxas a termo calculadas (tipo Float64).
            A primeira taxa de cada grupo corresponde à taxa zero inicial.

    Raises:
        ValueError: Se os índices de `bdays` e `rates` não forem iguais.
        ValueError: Se `groupby_dates` não for None e não tiver o mesmo tamanho

    Examples:
        >>> bdays = [10, 20, 30]
        >>> rates = [0.05, 0.06, 0.07]
        >>> yd.forwards(bdays, rates)
        0        0.05
        1    0.070095
        2    0.090284
        dtype: double[pyarrow]

        >>> # Exemplo com agrupamento (a última está isolada em outro grupo)
        >>> groupby_dates = [1, 1, 2]
        >>> yd.forwards(bdays, rates, groupby_dates)
        0    0.05
        1    0.070095
        2    0.07
        dtype: double[pyarrow]

        >>> # Exemplo com taxas indicativas de NTN-B em 16-09-2025
        >>> from pyield import ntnb
        >>> df = ntnb.data("16-09-2025")
        >>> yd.forwards(df["BDToMat"], df["IndicativeRate"])
        0       0.0943
        1     0.071549
        2     0.072439
        3     0.069558
        4     0.076614
        5     0.076005
        6     0.071325
        7     0.069915
        8     0.068105
        9     0.071278
        10    0.069117
        11    0.070373
        12    0.073286
        dtype: double[pyarrow]

        >>> # Valores nulos são descartados no cálculo e retornados como nulos
        >>> du = [230, 415, 730, None, 914]
        >>> tx = [0.0943, 0.084099, 0.079052, 0.1, 0.077134]
        >>> yd.forwards(du, tx)
        0      0.0943
        1    0.071549
        2    0.072439
        3        <NA>
        4    0.069558
        dtype: double[pyarrow]

        >>> # O algoritmo ordena os dados de entrada antes do cálculo e retorna
        >>> # os resultados na ordem original. Valores duplicados são tratados
        >>> # como um único ponto no cálculo da taxa a termo (último valor é mantido).
        >>> du = [230, 730, 415, 230]
        >>> tx = [0.1, 0.079052, 0.084099, 0.0943]
        >>> yd.forwards(du, tx)
        0      0.0943
        1    0.072439
        2    0.071549
        3      0.0943
        dtype: double[pyarrow]

    Note:
        - A função ordena os dados de entrada primeiro por `groupby_dates`,
        se for fornecido, e depois por `bdays` para garantir a ordem cronológica
        correta no cálculo das taxas a termo.
        - Os resultados são retornados na mesma ordem dos dados de entrada.
    """  # noqa: E501
    # 1. Montar o DataFrame
    # Criar coluna de agrupamento dummy se não for fornecida
    groupby_dates_exp = pl.Series(groupby_dates) if groupby_dates is not None else 0
    df_orig = pl.DataFrame(
        {
            "du_k": bdays,
            "rate_k": rates,
            "groupby_date": groupby_dates_exp,
        }
    )

    # 3. Definir a fórmula da taxa a termo
    # fₖ = fⱼ→ₖ = ((1 + rₖ)^tₖ / (1 + rⱼ)^tⱼ) ^ (1/(tₖ - tⱼ)) - 1
    exp1 = (1 + pl.col("rate_k")) ** pl.col("time_k")  # (1 + rₖ)^tₖ
    exp2 = (1 + pl.col("rate_j")) ** pl.col("time_j")  # (1 + rⱼ)^tⱼ
    exp3 = 1 / (pl.col("time_k") - pl.col("time_j"))  # 1/(tₖ - tⱼ)
    fwd_formula = (exp1 / exp2) ** exp3 - 1

    # --- Início da Lógica com Expressões (Lazy API) ---
    df_fwd = (
        df_orig.drop_nans()
        .drop_nulls()
        .unique(subset=["du_k", "groupby_date"], keep="last")
        .sort(["groupby_date", "du_k"])
        .with_columns(time_k=pl.col("du_k") / 252)  # Criar coluna de tempo em anos
        .with_columns(
            # Calcular os valores deslocados (shift) dentro de cada grupo
            rate_j=pl.col("rate_k").shift(1).over("groupby_date"),
            time_j=pl.col("time_k").shift(1).over("groupby_date"),
        )
        .with_columns(fwd=fwd_formula)
        .with_columns(
            # Usar a taxa spot para a primeira entrada de cada grupo
            fwd=pl.when(pl.col("time_j").is_null())
            .then(pl.col("rate_k"))
            .otherwise(pl.col("fwd"))
        )
    )
    s_fwd = (
        df_orig.join(
            df_fwd,
            on=["du_k", "groupby_date"],
            how="left",
            maintain_order="left",
        )
        .get_column("fwd")
        .to_pandas(use_pyarrow_extension_array=True)
    )

    s_fwd.name = None  # Remover o nome da série
    return s_fwd


def forward(
    bday1: int,
    bday2: int,
    rate1: float,
    rate2: float,
) -> float:
    r"""
    Calcula a taxa a termo (forward rate) entre dois prazos (dias úteis).

    Utiliza a fórmula:
        f₁→₂ = ((1 + r₂)^(du₂/252) / (1 + r₁)^(du₁/252))^(252/(du₂ - du₁)) - 1

    Onde:
        - r₁ é a taxa zero para o primeiro prazo (du₁).
        - r₂ é a taxa zero para o segundo prazo (du₂).
        - du₁ é o número de dias úteis até a primeira data.
        - du₂ é o número de dias úteis até a segunda data.
        - A constante 252 representa o número de dias úteis no ano.

    Como du/252 = t (tempo em anos úteis), a fórmula pode ser simplificada para:

        f₁→₂ = ((1 + r₂)^t₂ / (1 + r₁)^t₁)^(1/(t₂ - t₁)) - 1

    Args:
        bday1 (int): Número de dias úteis do primeiro ponto (prazo menor).
        bday2 (int): Número de dias úteis do segundo ponto (prazo maior).
        rate1 (float): Taxa zero (spot rate) para o prazo `bday1`.
        rate2 (float): Taxa zero (spot rate) para o prazo `bday2`.

    Returns:
        float: A taxa a termo calculada entre `bday1` e `bday2`. Retorna
            `float('nan')` se `bday2 <= bday1` ou se qualquer um dos
            argumentos de entrada for NaN.

    Examples:
        >>> # Exemplo válido: bday2 > bday1
        >>> yd.forward(10, 20, 0.05, 0.06)
        0.0700952380952371
        >>> # Exemplo inválido: bday2 <= bday1
        >>> yd.forward(20, 10, 0.06, 0.05)
        nan
        >>> yd.forward(10, 10, 0.05, 0.05)
        nan
        >>> # Exemplo com NaN na entrada
        >>> yd.forward(10, 20, 0.05, pd.NA)
        nan

    Note:
        É fundamental que `bday2` seja estritamente maior que `bday1` para que
        o cálculo da taxa a termo seja matematicamente válido.

    A fórmula utilizada é derivada da relação entre taxas zero (spot rates) é:
    $$
    f_{1 \rightarrow 2} = \left( \frac{(1 + r_2)^{t_2}}{(1 + r_1)^{t_1}} \right)^{\frac{1}{t_2 - t_1}} - 1
    $$
    """  # noqa: E501
    if pd.isna(rate1) or pd.isna(rate2) or pd.isna(bday1) or pd.isna(bday2):
        # If any of the inputs are NaN, return NaN
        return float("nan")

    # Handle the case where the two dates are the same
    if bday2 <= bday1:
        return float("nan")

    # Convert business days to business years
    t1 = bday1 / 252
    t2 = bday2 / 252

    # f₁→₂ = ((1 + r₂)^t₂ / (1 + r₁)^t₁)^(1/(t₂ - t₁)) - 1
    return ((1 + rate2) ** t2 / (1 + rate1) ** t1) ** (1 / (t2 - t1)) - 1
