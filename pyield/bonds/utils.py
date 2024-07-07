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
