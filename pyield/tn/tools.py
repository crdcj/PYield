from typing import overload

import numpy as np
import pandas as pd


@overload
def truncate(values: float, decimal_places: int) -> float: ...
@overload
def truncate(values: pd.Series, decimal_places: int) -> pd.Series: ...
def truncate(values: float | pd.Series, decimal_places: int) -> float | pd.Series:
    """
    Truncate a float or a Pandas Series to the specified decimal place.

    Args:
        values (float or pandas.Series): The value(s) to be truncated.
        decimal_places (int): The number of decimal places to truncate to.

    Returns:
        float or pandas.Series: The truncated value(s).
    """
    factor = 10**decimal_places
    truncated_values = np.trunc(values * factor) / factor
    if isinstance(truncated_values, np.floating):
        truncated_values = float(truncated_values)
    else:
        truncated_values = pd.Series(truncated_values)
    return truncated_values


def calculate_present_value(
    cash_flows: pd.Series,
    rates: pd.Series,
    periods: pd.Series,
) -> float:
    # Return 0 if any input is empty
    if cash_flows.empty or rates.empty or periods.empty:
        return 0

    # Reset the index to avoid issues with the series alignment
    cash_flows = cash_flows.reset_index(drop=True)
    rates = rates.reset_index(drop=True)
    periods = periods.reset_index(drop=True)

    # Check if data have the same length
    if len(cash_flows) != len(rates) or len(cash_flows) != len(periods):
        raise ValueError("All series must have the same length.")

    return (cash_flows / (1 + rates) ** periods).sum()
