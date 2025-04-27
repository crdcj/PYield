import logging
from datetime import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd

import pyield.date_converter as dc
from pyield import b3, bday, interpolator
from pyield.data_cache import get_cached_dataset
from pyield.date_converter import DateArray, DateScalar

logger = logging.getLogger(__name__)

TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")


def data(
    date: DateScalar,
    month_start: bool = False,
    pre_filter: bool = False,
    all_columns: bool = True,
) -> pd.DataFrame:
    """
    Retrieves DI Futures contract data for a specific trade date.

    Provides access to DI futures data, allowing adjustments to expiration dates
    (to month start) and optional filtering based on LTN and NTN-F bond maturities.

    Args:
        date (DateScalar): The trade date for which to retrieve DI contract data.
            If the date is invalid, a holiday, in the future, or None, an empty
            DataFrame is returned with appropriate logging.
        month_start (bool, optional): If True, adjusts all expiration dates to the
            first day of their respective month (e.g., 2025-02-01 becomes
            2025-01-01). Defaults to False.
        pre_filter (bool, optional): If True, filters DI contracts to include only
            those whose expiration dates match known prefixed Treasury bond (LTN, NTN-F)
            maturities from the TPF dataset nearest to the given trade date.
            Defaults to False.
        all_columns (bool, optional): If True, returns all available columns from
            the DI dataset. If False, returns a subset of the most common columns.
            Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame containing the DI futures contract data for the
            specified date, sorted by expiration date. Returns an empty DataFrame
            if no data is found, the date is invalid, a holiday, or in the future.

    Examples:
        >>> from pyield import dif
        >>> df = dif.data(date="16-10-2024", month_start=True)
        >>> df.iloc[:5, :5]  # Show the first five rows and columns
           TradeDate ExpirationDate TickerSymbol  BDaysToExp  OpenContracts
        0 2024-10-16     2024-11-01       DI1X24          12        1744269
        1 2024-10-16     2024-12-01       DI1Z24          31        1429375
        2 2024-10-16     2025-01-01       DI1F25          52        5423969
        3 2024-10-16     2025-02-01       DI1G25          74         279491
        4 2024-10-16     2025-03-01       DI1H25          94         344056
    """
    if date:
        date = dc.convert_input_dates(date)
        bz_today = dt.now(TIMEZONE_BZ).date()
        if date.date() > bz_today:
            logger.warning(f"DI date ({date}) after current date {bz_today}")
            logger.warning("Returning empty DataFrame.")
            return pd.DataFrame()
    else:
        logger.info("No date specified. Returning empty DataFrame.")
        return pd.DataFrame()
    # Return an empty DataFrame if the trade date is a holiday
    if not bday.is_business_day(date):
        logger.warning("Specified date is not a business day.")
        logger.warning("Returning empty DataFrame.")
        return pd.DataFrame()

    # Get historical data
    df = get_cached_dataset("DI").query("TradeDate == @date").reset_index(drop=True)

    if df.empty:
        logger.info("No historical data found. Trying real-time data.")
        df = b3.futures(contract_code="DI1", date=date)

    if df.empty:
        logger.warning("No DI Futures data found for the specified date.")
        logger.warning("Returning empty DataFrame.")
        return pd.DataFrame()

    if "DaysToExpiration" in df.columns:
        df.drop(columns=["DaysToExpiration"], inplace=True)

    if pre_filter:
        df_pre = (
            get_cached_dataset("TPF")
            .query("BondType in ['LTN', 'NTN-F']")
            .reset_index(drop=True)[["ReferenceDate", "MaturityDate"]]
        )

        nearest_date = _find_nearest_date(date, df_pre["ReferenceDate"])
        if nearest_date is pd.NaT:
            logger.warning("No matching reference date found in TPF dataset.")
            return pd.DataFrame()

        # Filter the TPF dataset for the nearest reference date
        pre_maturities = (
            df_pre.query("ReferenceDate == @nearest_date")["MaturityDate"]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Force the expirations to be a business day as DI contracts
        pre_maturities = bday.offset(pre_maturities, 0)
        df = df.query("ExpirationDate in @pre_maturities").reset_index(drop=True)

    if month_start:
        df["ExpirationDate"] = df["ExpirationDate"].dt.to_period("M").dt.to_timestamp()

    if not all_columns:
        cols = [
            "TradeDate",
            "TickerSymbol",
            "ExpirationDate",
            "BDaysToExp",
            "OpenContracts",
            "TradeVolume",
            "DV01",
            "SettlementPrice",
            "LastPrice",
            "OpenRate",
            "MinRate",
            "MaxRate",
            "CloseRate",
            "SettlementRate",
            "LastRate",
            "ForwardRate",
        ]
        selected_cols = [col for col in cols if col in df.columns]
        df = df[selected_cols].copy()

    return df.sort_values(by=["TradeDate", "ExpirationDate"]).reset_index(drop=True)


def _find_nearest_date(
    target_date: pd.Timestamp, date_series: pd.Series
) -> pd.Timestamp:
    """Finds the date in a Series closest to the target date.

    Args:
        target_date (pd.Timestamp): The reference date.
        date_series (pd.Series): A Series of dates to search within.

    Returns:
        pd.Timestamp: The date from `date_series` that is closest in time
            to `target_date`. Returns pd.NaT if the series is empty or
            target_date is None.
    """
    if date_series.empty or target_date is None:
        return pd.NaT
    # The result will be a Series of positive Timedeltas (durations)
    abs_differences = (date_series - target_date).abs()

    # Find the index of the minimum difference in the differences Series
    # idxmin() returns the index label where the minimum value occurs
    closest_index = abs_differences.idxmin()

    # Use the found index to get the corresponding date from the original Series
    return date_series.loc[closest_index]  # or date_series[closest_index]


def interpolate_rates(
    dates: DateScalar | DateArray,
    expirations: DateScalar | DateArray,
    extrapolate: bool = True,
) -> pd.Series:
    """
    Interpolates DI rates for specified trade dates and expiration dates.

    Calculates interpolated DI rates using the flat-forward method for given
    sets of trade dates and expiration dates. This function is well-suited
    for vectorized calculations across multiple date pairs.

    If DI rates are unavailable for a given trade date, the corresponding
    interpolated rate(s) will be NaN.

    Handles broadcasting: If one argument is a scalar and the other is an array,
    the scalar value is applied to all elements of the array.

    Args:
        dates (DateScalar | DateArray): The trade date(s) for the rates.
        expirations (DateScalar | DateArray): The corresponding expiration date(s).
            Must be compatible in length with `dates` if both are arrays.
        extrapolate (bool, optional): Whether to allow extrapolation beyond the
            range of known DI rates for a given trade date. Defaults to True.

    Returns:
        pd.Series: A Series containing the interpolated DI rates (as floats).
            Values will be NaN where interpolation is not possible
            (e.g., no DI data for the trade date).

    Raises:
        ValueError: If `dates` and `expirations` are both array-like but have
            different lengths.
    """
    # Convert input dates to a consistent format
    dates = dc.convert_input_dates(dates)
    expirations = dc.convert_input_dates(expirations)

    # Ensure the lengths of input arrays are consistent
    match (dates, expirations):
        case pd.Timestamp(), pd.Series():
            dfi = pd.DataFrame({"mat": expirations})
            dfi["tdate"] = dates

        case pd.Series(), pd.Timestamp():
            dfi = pd.DataFrame({"tdate": dates})
            dfi["mat"] = expirations

        case pd.Series(), pd.Series():
            if len(dates) != len(expirations):
                raise ValueError("Args. should have the same length.")
            dfi = pd.DataFrame({"tdate": dates, "mat": expirations})

        case pd.Timestamp(), pd.Timestamp():
            dfi = pd.DataFrame({"tdate": [dates], "mat": [expirations]})

    # Compute business days between reference dates and maturities
    dfi["bdays"] = bday.count(dfi["tdate"], dfi["mat"])

    # Initialize the interpolated rate column with NaN
    dfi["irate"] = pd.NA
    dfi["irate"] = dfi["irate"].astype("Float64")

    # Load DI rates dataset filtered by the provided reference dates
    dfr = (
        get_cached_dataset("DI")
        .query("TradeDate in @dfi['tdate'].unique()")
        .reset_index(drop=True)
    )

    # Return an empty DataFrame if no rates are found
    if dfr.empty:
        return pd.Series()

    # Iterate over each unique reference date
    for date in dfi["tdate"].unique():
        # Filter DI rates for the current reference date
        dfr_subset = dfr.query("TradeDate == @date").reset_index(drop=True)

        # Skip processing if no rates are available for the current date
        if dfr_subset.empty:
            continue

        # Initialize the interpolator with known rates and business days
        interp = interpolator.Interpolator(
            method="flat_forward",
            known_bdays=dfr_subset["BDaysToExp"],
            known_rates=dfr_subset["SettlementRate"],
            extrapolate=extrapolate,
        )

        # Apply interpolation to rows matching the current reference date
        mask: pd.Series = dfi["tdate"] == date
        dfi.loc[mask, "irate"] = dfi.loc[mask, "bdays"].apply(interp)

    # Return the Series with interpolated rates
    dfi["irate"].name = None
    return dfi["irate"]


def interpolate_rate(
    date: DateScalar,
    expiration: DateScalar,
    extrapolate: bool = False,
) -> float:
    """
    Interpolates or retrieves the DI rate for a single expiration date.

    Fetches DI contract data for the specified trade `date` and determines the
    settlement rate for the given `expiration`. If an exact match for the
    expiration date exists, its rate is returned. Otherwise, the rate is
    interpolated using the flat-forward method based on the rates of surrounding
    contracts.

    Args:
        date (DateScalar): The trade date for which to retrieve DI data.
        expiration (DateScalar): The target expiration date for the rate.
        extrapolate (bool, optional): If True, allows extrapolation if the
            `expiration` date falls outside the range of available contract
            expirations for the given `date`. Defaults to False.

    Returns:
        float: The exact or interpolated DI settlement rate for the specified
            date and expiration. Returns `float('NaN')` if:
                - No DI data is found for the `date`.
                - The `expiration` is outside range and `extrapolate` is False.
                - An interpolation calculation fails.

    Examples:
        >>> from pyield import dif
        >>> # Get rate for an existing contract expiration
        >>> dif.interpolate_rate("25-04-2025", "01-01-2027")
        0.13901

        >>> # Get rate for a non-existing contract expiration
        >>> dif.interpolate_rate("25-04-2025", "01-11-2027")
        0.13576348733268917

        >>> # Extrapolate rate for a future expiration date
        >>> dif.interpolate_rate("25-04-2025", "01-01-2050", extrapolate=True)
        0.13881
    """

    expiration = dc.convert_input_dates(expiration)
    if not date:
        return float("NaN")

    # Get the DI contract DataFrame
    df = data(date=date)

    if df.empty:
        return float("NaN")

    max_exp = df["ExpirationDate"].max()

    if expiration > max_exp and not extrapolate:
        logger.warning(
            f"Expiration date ({expiration}) is greater than the maximum expiration "
            f"date ({max_exp}) and extrapolation is not allowed. Returning NaN."
        )
        return float("NaN")

    if expiration in df["ExpirationDate"]:
        rate = df.query("ExpirationDate == @expiration")["SettlementRate"]
        return float(rate.iloc[0]) if not rate.empty else float("NaN")

    ff_interp = interpolator.Interpolator(
        method="flat_forward",
        known_bdays=df["BDaysToExp"],
        known_rates=df["SettlementRate"],
        extrapolate=extrapolate,
    )
    bd = bday.count(date, expiration)
    return ff_interp(bd)


def eod_dates() -> pd.Series:
    """
    Returns all unique end-of-day trade dates available in the DI dataset.

    Retrieves and lists all distinct 'TradeDate' values present in the
    historical DI futures data cache, sorted chronologically.

    Returns:
        pd.Series: A sorted Series of unique trade dates (pd.Timestamp)
                   for which DI data is available.

    Examples:
        >>> from pyield import dif
        >>> # DI Futures series starts from 1995-01-02
        >>> dif.eod_dates().head(5)
        0   1995-01-02
        1   1995-01-03
        2   1995-01-04
        3   1995-01-05
        4   1995-01-06
        dtype: datetime64[ns]
    """
    available_dates = (
        get_cached_dataset("DI")
        .drop_duplicates(subset=["TradeDate"])["TradeDate"]
        .sort_values(ascending=True)
        .reset_index(drop=True)
    )
    available_dates.name = None
    return available_dates
