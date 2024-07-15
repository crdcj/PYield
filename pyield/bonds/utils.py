import numpy as np
import pandas as pd


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
    if isinstance(truncated_values, np.float64):
        truncated_values = float(truncated_values)
    else:
        truncated_values = pd.Series(truncated_values)
    return truncated_values


def calculate_present_value(
    cash_flows: pd.Series,
    discount_rates: pd.Series,
    time_periods: pd.Series,
) -> float:
    return (cash_flows / (1 + discount_rates) ** time_periods).sum()


def standardize_rates(rates: pd.Series) -> pd.Series:
    if not isinstance(rates.index, pd.DatetimeIndex):
        raise ValueError("The rates index must be a DatetimeIndex with maturity dates.")
    return rates.dropna().sort_index()
