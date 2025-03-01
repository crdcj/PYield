import pandas as pd


def forward_rates(
    business_days: pd.Series,
    zero_rates: pd.Series,
    groupby_dates: pd.Series | None = None,
) -> pd.Series:
    """Calculates forward rates from zero rates.

    Args:
        business_days (pd.Series): Number of business days for each zero rate.
        zero_rates (pd.Series): Zero rates corresponding to the business days.
        groupby_dates (pd.Series | None, optional): Optional grouping criteria to
            segment calculations. If not provided, calculations will not be grouped.

    Returns:
        pd.Series: Series of calculated forward rates with the first rate set to the
            corresponding zero rate.
    """
    # Reset Series indexes to avoid misalignment issues during calculations
    business_days = business_days.reset_index(drop=True)
    zero_rates = zero_rates.reset_index(drop=True)
    if groupby_dates is not None:
        groupby_dates = groupby_dates.reset_index(drop=True)

    # Create a DataFrame to work with the given series
    df = pd.DataFrame({"bd": business_days, "zero_rate": zero_rates})

    # If no groupby_dates is provided, create a dummy column to group the DataFrame
    if groupby_dates is not None:
        df["groupby_date"] = groupby_dates
    else:
        df["groupby_date"] = 0  # Dummy value to group the DataFrame

    # Sort by the groupby_dates and bd columns to ensure proper chronological order
    df.sort_values(by=["groupby_date", "bd"], inplace=True)

    # Calculate the next zero rate and business day for each group
    df["next_rate"] = df.groupby("groupby_date")["zero_rate"].shift(1)
    df["next_bday"] = df.groupby("groupby_date")["bd"].shift(1)

    # Calculate the forward rates using the formula
    factor1 = (1 + df["next_rate"]) ** (df["next_bday"] / 252)
    factor2 = (1 + df["zero_rate"]) ** (df["bd"] / 252)
    factor3 = 252 / (df["next_bday"] - df["bd"])
    df["fwd_rate"] = (factor1 / factor2) ** factor3 - 1

    # Identifify the first index of each group of dates
    first_indices = df.groupby("groupby_date").head(1).index
    # Set the first forward rate of each group to the zero rate
    df.loc[first_indices, "fwd_rate"] = df.loc[first_indices, "zero_rate"]

    df["fwd_rate"] = df["fwd_rate"].astype("Float64")

    return df["fwd_rate"]
