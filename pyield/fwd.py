import pandas as pd


def forwards(
    bdays: pd.Series,
    rates: pd.Series,
    groupby_dates: pd.Series | None = None,
) -> pd.Series:
    r"""
    Calcula taxas a termo (forward rates) a partir de taxas zero (spot rates).

    A taxa a termo no vértice 'n' é definida como:
        fwdₙ = fwdₙ₋₁→ₙ (a taxa a termo de n-1 para n)

    A fórmula utilizada é:
        fwdₙ = ((1 + rₙ)^(duₙ/252) / (1 + rₙ₋₁)^(duₙ₋₁/252))^(252/(duₙ - duₙ₋₁)) - 1

    Como du/252 = t (tempo em anos úteis), a fórmula pode ser simplificada para:

        fwdₙ = ((1 + rₙ)^tₙ / (1 + rₙ₋₁)^tₙ₋₁)^(1/(tₙ - tₙ₋₁)) - 1

    Em LaTeX, a fórmula é representada como:
    $$
    fwd_{n} = \left( \frac{(1 + r_n)^{t_n}}{(1 + r_{n-1})^{t_{n-1}}} \right)^{\frac{1}{t_n - t_{n-1}}} - 1
    $$

    Onde:
        - rₙ₋₁ é a taxa zero para o vértice anterior.
        - rₙ é a taxa zero para o vértice atual.
        - tₙ₋₁ é o prazo em anos para o vértice anterior (calculado como duₙ₋₁/252).
        - tₙ é o prazo em anos para o vértice atual (calculado como duₙ/252).
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
    """  # noqa: E501
    bdays = bdays.astype("Int64")
    rates = rates.astype("Float64")

    # Check if indexes are the same
    if not bdays.index.equals(rates.index):
        raise ValueError("The indexes of bdays and rates must be the same.")

    if groupby_dates:
        if groupby_dates.index.equals(bdays.index):
            raise ValueError("groupby_dates index must be the same as bdays and rates.")

    # Create a DataFrame to work with the given series
    df = pd.DataFrame({"du2": bdays, "r2": rates})
    df["t2"] = df["du2"] / 252

    # If no groupby_dates is provided, create a dummy column to group the DataFrame
    if groupby_dates is not None:
        df["groupby_date"] = groupby_dates
    else:
        df["groupby_date"] = 0  # Dummy value to group the DataFrame

    # Sort by the groupby_dates and t2 columns to ensure proper chronological order
    df.sort_values(by=["groupby_date", "t2"], inplace=True)

    # Calculate the next zero rate and business day for each group
    df["r1"] = df.groupby("groupby_date")["r2"].shift(1)
    df["t1"] = df.groupby("groupby_date")["t2"].shift(1)

    # Calculate the formula components
    factor_r2 = (1 + df["r2"]) ** df["t2"]  # (1 + r₂)^t₂
    factor_r1 = (1 + df["r1"]) ** df["t1"]  # (1 + r₁)^t₁
    time_exp = 1 / (df["t2"] - df["t1"])  # 1/(t₂ - t₁)

    # f₁→₂ = ((1 + r₂)^t₂ / (1 + r₁)^t₁)^(1/(t₂ - t₁)) - 1
    df["f1_2"] = (factor_r2 / factor_r1) ** time_exp - 1

    # Identifify the first index of each group of dates
    first_indices = df.groupby("groupby_date").head(1).index
    # Set the first forward rate of each group to the zero rate
    df.loc[first_indices, "f1_2"] = df.loc[first_indices, "r2"]

    # Return the forward rates as a Series
    f1_2 = df["f1_2"]
    f1_2.name = None
    return f1_2


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
