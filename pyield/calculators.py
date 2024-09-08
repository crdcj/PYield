import numpy as np
import pandas as pd


def forward_rates(
    bdays: pd.Series,
    zero_rates: pd.Series,
    groupby_dates: pd.Series | None = None,
) -> pd.Series:
    """Calculates forward rates from zero rates.

    Args:
        bdays (pd.Series): Number of business days corresponding to the zero rates.
        zero_rates (pd.Series): Zero rates corresponding to the business days.
        groupby_dates (pd.Series | None, optional): Optional grouping criteria to
            segment calculations. If not provided, calculations will not be grouped.

    Returns:
        pd.Series: Series of calculated forward rates with the last rate set to the last
            zero rate in the input series.
    """
    # Reset Series indexes to avoid misalignment issues during calculations
    bdays = bdays.reset_index(drop=True)
    zero_rates = zero_rates.reset_index(drop=True)
    if groupby_dates is not None:
        groupby_dates = groupby_dates.reset_index(drop=True)

    # Create a DataFrame to work with the given series
    df = pd.DataFrame({"bdays": bdays, "zero_rates": zero_rates})

    # If no groupby_dates is provided, create a dummy column to group the DataFrame
    if groupby_dates is not None:
        df["groupby_dates"] = groupby_dates
    else:
        df["groupby_dates"] = 0

    # Sort by the groupby_dates and bdays columns to ensure proper chronological order
    df.sort_values(by=["groupby_dates", "bdays"], inplace=True)

    # Calculate the next zero rate and business day for each group
    df["next_rate"] = df.groupby("groupby_dates")["zero_rates"].shift(-1)
    df["next_bday"] = df.groupby("groupby_dates")["bdays"].shift(-1)

    # Calculate the forward rates using the formula
    factor1 = (1 + df["next_rate"]) ** (df["next_bday"] / 252)
    factor2 = (1 + df["zero_rates"]) ** (df["bdays"] / 252)
    factor3 = 252 / (df["next_bday"] - df["bdays"])
    df["fwd_rates"] = (factor1 / factor2) ** factor3 - 1

    # Replace the last forward rate with the last zero rate
    mask = df["zero_rates"].notnull() & (df["fwd_rates"].isnull())
    df["fwd_rates"] = np.where(mask, df["zero_rates"], df["fwd_rates"])

    df["fwd_rates"] = df["fwd_rates"].astype("Float64")

    return df["fwd_rates"]
