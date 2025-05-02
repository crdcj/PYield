import pandas as pd


def forwards(
    bdays: pd.Series,
    rates: pd.Series,
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
        >>> bdays = pd.Series([10, 20, 30])
        >>> rates = pd.Series([0.05, 0.06, 0.07])
        >>> yd.forwards(bdays, rates)
        0        0.05
        1    0.070095
        2    0.090284
        dtype: Float64

        >>> # Exemplo com agrupamento (a última está isolada em outro grupo)
        >>> groupby_dates = pd.Series([1, 1, 2])
        >>> yd.forwards(bdays, rates, groupby_dates)
        0    0.05
        1    0.070095
        2    0.07
        dtype: Float64

    Note:
        - A função ordena os dados de entrada primeiro por `groupby_dates`,
        se for fornecido, e depois por `bdays` para garantir a ordem cronológica
        correta no cálculo das taxas a termo.
        - Os resultados são retornados na mesma ordem dos dados de entrada.
    """  # noqa: E501
    bdays = bdays.astype("Int64")
    rates = rates.astype("Float64")

    # Check if indexes are the same
    if not bdays.index.equals(rates.index):
        raise ValueError("The indexes of bdays and rates must be the same.")

    # Store original index
    original_index = bdays.index

    # Create a DataFrame to work with the given series
    df = pd.DataFrame({"du_k": bdays, "rate_k": rates})
    df["time_k"] = df["du_k"] / 252

    if isinstance(groupby_dates, pd.Series):
        if not groupby_dates.index.equals(bdays.index):
            raise ValueError("groupby_dates index must be the same as bdays and rates.")
        df["groupby_date"] = groupby_dates
    else:
        df["groupby_date"] = 0  # Dummy value to group the DataFrame

    # Sort by the groupby_dates and t2 columns to ensure proper chronological order
    df = df.sort_values(by=["groupby_date", "time_k"])

    # Calculate the next zero rate and business day for each group
    df["rate_j"] = df.groupby("groupby_date")["rate_k"].shift(1)
    df["time_j"] = df.groupby("groupby_date")["time_k"].shift(1)

    # Calculate the formula components
    # fₖ = fⱼ→ₖ = ((1 + rₖ)^tₖ / (1 + rⱼ)^tⱼ) ^ (1/(tₖ - tⱼ)) - 1
    factor_k = (1 + df["rate_k"]) ** df["time_k"]  # (1 + rₖ)^tₖ
    factor_j = (1 + df["rate_j"]) ** df["time_j"]  # (1 + rⱼ)^tⱼ
    factor_t = 1 / (df["time_k"] - df["time_j"])  # 1/(tₖ - tⱼ)
    df["fwd"] = (factor_k / factor_j) ** factor_t - 1

    # Identifify the first index of each group of dates
    first_indices = df.groupby("groupby_date").head(1).index
    # Set the first forward rate of each group to the zero rate
    df.loc[first_indices, "fwd"] = df.loc[first_indices, "rate_k"]

    # Reindex the result to match the original input order
    result_fwd = df["fwd"].reindex(original_index)

    # Return the forward rates as a Series with no name
    result_fwd.name = None
    return result_fwd


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
