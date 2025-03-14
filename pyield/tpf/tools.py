from typing import overload

import numpy as np
import pandas as pd

from pyield import date_converter as dc
from pyield.anbima import tpf
from pyield.b3.di import DIFutures
from pyield.date_converter import DateScalar


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


def pre_spreads(date: DateScalar) -> pd.DataFrame:
    """
    Calculates the DI spread for Brazilian treasury bonds (LTN and NTN-F) based on
    ANBIMA's indicative rates.

    This function fetches the indicative rates for Brazilian treasury securities (LTN
    and NTN-F bonds) and the DI futures rates for a specified reference date,
    calculating the spread between these rates in basis points. If no reference date is
    provided, the function uses the previous business day.

    Parameters:
        date (DateScalar): The reference date for the spread calculation.

    Returns:
        pd.DataFrame: DataFrame containing the bond type, maturity date and the
            calculated spread in basis points.
    """
    # Fetch DI rates for the reference date
    converted_date = dc.convert_input_dates(date)
    di = DIFutures(date=converted_date, month_start=True)
    df_di = di.df
    if "SettlementRate" not in df_di.columns or df_di.empty:
        raise ValueError("DI data is missing the 'SettlementRate' column or is empty.")

    df_di = df_di[["ExpirationDate", "SettlementRate"]].copy()

    # Renaming the columns to match the ANBIMA structure
    df_di.rename(columns={"ExpirationDate": "MaturityDate"}, inplace=True)

    # Fetch bond rates, filtering for LTN and NTN-F types
    df_ltn = tpf.tpf_rates(converted_date, "LTN")
    df_ntnf = tpf.tpf_rates(converted_date, "NTN-F")
    df_pre = pd.concat([df_ltn, df_ntnf], ignore_index=True)

    # Merge bond and DI rates by maturity date to calculate spreads
    df_spreads = pd.merge(df_pre, df_di, how="left", on="MaturityDate")

    # Calculate the DI spread as the difference between indicative and settlement rates
    df_spreads["DISpread"] = df_spreads["IndicativeRate"] - df_spreads["SettlementRate"]

    # Convert spread to basis points for clarity
    df_spreads["DISpread"] = (10_000 * df_spreads["DISpread"]).round(2)

    # Prepare and return the final sorted DataFrame
    df_spreads = df_spreads.sort_values(["BondType", "MaturityDate"], ignore_index=True)
    return df_spreads[["BondType", "MaturityDate", "DISpread"]].copy()
