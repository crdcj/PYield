from typing import Literal

import pandas as pd

from . import date_validator as dv
from .fetchers.anbima import anbima
from .fetchers.futures import futures

# Constant for conversion to basis points
BPS_CONVERSION_FACTOR = 10_000


def spread(
    spread_type: Literal["DI_PRE"],
    reference_date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Calculate the spread between different types of rates for a specified date.

    This function calculates the spread between different types of rates for a specified
    reference date. The available spread types are:
    - 'DI': The spread between the indicative rate for Brazilian treasury bonds (LTN and
      NTN-F) and the DI futures rate.

    Parameters:
        spread_type (str): The type of spread to calculate. Must be one of 'DI_PRE'.
        reference_date (str | pd.Timestamp, optional): The reference date for the spread
            calculation. If None or not provided, defaults to the previous business day
            according to the Brazilian calendar.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated spread for the specified
            reference date. The data is sorted by bond type and maturity date.

    Raises:
        ValueError: If an unsupported spread type is provided.

    Example:
        >>> spread("DI_PRE", "2024-06-18")
    """
    spread_type_cap = str(spread_type).upper()
    normalized_date = dv.normalize_date(reference_date)
    if spread_type_cap == "DI_PRE":
        return di_pre(normalized_date)
    else:
        raise ValueError(f"Unsupported spread type: {spread_type}")


def di_pre(reference_date: pd.Timestamp) -> pd.DataFrame:
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
    df_di = futures("DI1", reference_date)[["ExpirationDate", "SettlementRate"]]

    # Renaming the columns to match the ANBIMA structure
    df_di.rename(columns={"ExpirationDate": "MaturityDate"}, inplace=True)

    # Adjusting maturity date to match bond data format
    df_di["MaturityDate"] = df_di["MaturityDate"].dt.to_period("M").dt.to_timestamp()

    # Fetch bond rates, filtering for LTN and NTN-F types
    df_anbima = anbima(["LTN", "NTN-F"], reference_date)
    # Keep only the relevant columns for the output
    keep_columns = ["ReferenceDate", "BondType", "MaturityDate", "IndicativeRate"]
    df_anbima = df_anbima[keep_columns].copy()

    # Merge bond and DI rates by maturity date to calculate spreads
    df_final = pd.merge(df_anbima, df_di, how="left", on="MaturityDate")

    # Calculate the DI spread as the difference between indicative and settlement rates
    df_final["DISpread"] = df_final["IndicativeRate"] - df_final["SettlementRate"]

    # Convert spread to basis points for clarity
    df_final["DISpread"] = (BPS_CONVERSION_FACTOR * df_final["DISpread"]).round(2)

    # Prepare and return the final sorted DataFrame
    select_columns = ["BondType", "ReferenceDate", "MaturityDate", "DISpread"]
    df_final = df_final[select_columns].copy()
    return df_final.sort_values(["BondType", "MaturityDate"], ignore_index=True)
