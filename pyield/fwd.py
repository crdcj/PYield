import pandas as pd


def forwards(
    bdays: pd.Series,
    rates: pd.Series,
    groupby_dates: pd.Series | None = None,
) -> pd.Series:
    """
    Calculates forward rates from zero rates using the formula:
        f₁→₂ = ((1 + r₂)^(du₂/252) / (1 + r₁)^(du₁/252))^(252/(du₂ - du₁)) - 1

    Since du/252 = t, the formula can be simplified to:
        f₁→₂ = ((1 + r₂)^t₂ / (1 + r₁)^t₁)^(1/(t₂ - t₁)) - 1

    where:
        - r₁ is the zero rate for the previous period
        - r₂ is the zero rate for the current period
        - t₁ is the time in years for the previous period
        - t₂ is the time in years for the current period

    The first forward rate is set to the corresponding zero rate.
    This function can also handle grouping of the input data based on the
    `groupby_dates` parameter. If provided, the calculations will be performed
    separately for each group, allowing for different forward rate calculations
    based on the grouping criteria.

    Args:
        bdays (pd.Series): Number of business days for each zero rate.
        rates (pd.Series): Zero rates corresponding to the business days.
        groupby_dates (pd.Series | None, optional): Optional grouping criteria to
            segment calculations. If not provided, calculations will not be grouped.

    Returns:
        pd.Series: Series of calculated forward rates with the first rate set to the
            corresponding zero rate.
    """
    # Reset Series indexes to avoid misalignment issues during calculations
    bdays = bdays.reset_index(drop=True)
    rates = rates.reset_index(drop=True)
    if groupby_dates is not None:
        groupby_dates = groupby_dates.reset_index(drop=True)

    # Create a DataFrame to work with the given series
    df = pd.DataFrame({"du2": bdays, "r2": rates})
    df["t2"] = df["du2"] / 252

    # If no groupby_dates is provided, create a dummy column to group the DataFrame
    if groupby_dates is not None:
        df["groupby_date"] = groupby_dates
    else:
        df["groupby_date"] = 0  # Dummy value to group the DataFrame

    # Sort by the groupby_dates and bd columns to ensure proper chronological order
    df.sort_values(by=["groupby_date", "t2"], inplace=True)

    # GetCalculate the next zero rate and business day for each group
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

    # Return the forward rates as a Series converting to Float64 to handle NaN values
    return df["f1_2"].astype("Float64")


def forward(
    bday1: int,
    bday2: int,
    rate1: float,
    rate2: float,
) -> float:
    """
    Calculates the forward rate between two business days using the formula:
        f₁→₂ = ((1 + r₂)^(du₂/252) / (1 + r₁)^(du₁/252))^(252/(du₂ - du₁)) - 1

    where:
        - r₁ is the zero rate for the previous period
        - r₂ is the zero rate for the current period
        - du₁ is the number of business days until the first date
        - du₂ is the number of business days until the second date

    Args:
        bday1 (int): Number of business days until the first date.
        bday2 (int): Number of business days until the second date.
        rate1 (float): Zero rate for the first date.
        rate2 (float): Zero rate for the second date.

    Returns:
        float: The calculated forward rate.

    Example:
        >>> forward(10, 20, 0.05, 0.06)
        0.0700952380952371
    """
    if pd.isna(rate1) or pd.isna(rate2) or pd.isna(bday1) or pd.isna(bday2):
        # If any of the inputs are NaN, return NaN
        return float("nan")

    # Handle the case where the two dates are the same
    if bday1 == bday2:
        return float("nan")

    # Convert business days to business years
    t1 = bday1 / 252
    t2 = bday2 / 252

    # f₁→₂ = ((1 + r₂)^t₂ / (1 + r₁)^t₁)^(1/(t₂ - t₁)) - 1
    return ((1 + rate2) ** t2 / (1 + rate1) ** t1) ** (1 / (t2 - t1)) - 1
