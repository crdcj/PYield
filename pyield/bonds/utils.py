from typing import overload

import numpy as np
import pandas as pd

from .. import fetchers as ft


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


def di_spreads(reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Calculates the DI spread for Brazilian treasury bonds (LTN and NTN-F) based on
    ANBIMA's indicative rates.

    This function fetches the indicative rates for Brazilian treasury securities (LTN
    and NTN-F bonds) and the DI futures rates for a specified reference date,
    calculating the spread between these rates in basis points. If no reference date is
    provided, the function uses the previous business day.

    Parameters:
        reference_date (str | pd.Timestamp, optional): The reference date for the
            spread calculation. If None or not provided, defaults to the previous
            business day according to the Brazilian calendar.

    Returns:
        pd.DataFrame: A DataFrame containing the bond type, reference date, maturity
            date, and the calculated spread in basis points. The data is sorted by
            bond type and maturity date.
    """
    # Fetch DI rates for the reference date
    df_di = ft.futures("DI1", reference_date)[["ExpirationDate", "SettlementRate"]]

    # Renaming the columns to match the ANBIMA structure
    df_di.rename(columns={"ExpirationDate": "MaturityDate"}, inplace=True)

    # Adjusting maturity date to match bond data format
    df_di["MaturityDate"] = df_di["MaturityDate"].dt.to_period("M").dt.to_timestamp()

    # Fetch bond rates, filtering for LTN and NTN-F types
    df_ltn = ft.anbima_rates(reference_date, "LTN")
    df_ntnf = ft.anbima_rates(reference_date, "NTN-F")
    df_pre = pd.concat([df_ltn, df_ntnf], ignore_index=True)

    # Merge bond and DI rates by maturity date to calculate spreads
    df_spreads = pd.merge(df_pre, df_di, how="left", on="MaturityDate")

    # Calculate the DI spread as the difference between indicative and settlement rates
    df_spreads["DISpread"] = df_spreads["IndicativeRate"] - df_spreads["SettlementRate"]

    # Convert spread to basis points for clarity
    df_spreads["DISpread"] = (10_000 * df_spreads["DISpread"]).round(2)

    # Prepare and return the final sorted DataFrame
    select_columns = ["BondType", "ReferenceDate", "MaturityDate", "DISpread"]
    return df_spreads[select_columns].sort_values(["MaturityDate"], ignore_index=True)
