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
    Function to retrieve DI Futures contract data.

    This Function provides access to DI futures data for a specified trade date, and
    includes options to adjust expiration dates and apply filters based on LTN and
    NTN-F bond maturities.

    Args:
        date (DateScalar | None): The trade date to retrieve the
            DI contract data. If None, an empty DataFrame is returned.
            month_start (bool): If True, all expiration dates are adjusted to the first
            day of the month. For example, an expiration date of 02/01/2025 will be
            adjusted to 01/01/2025.
        pre_filter (bool): If True, filters the DI contracts to match only
            expirations with existing prefixed TN bond maturities (LTN and NTN-F).
        all_columns (bool): If True, returns all available columns in the DI dataset.
            If False, only the most common columns are returned.

    Examples:
        >>> df = yd.di.data(date="16-10-2024", month_start=True)
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
    Interpolates DI rates for specified trade dates and maturities. The method
    is recomended to be used in datasets calculations with multiple dates.

    This method calculates interpolated DI rates for a given set of trade
    dates and maturities using a flat-forward interpolation method. If no DI
    rates are available for a reference date, the interpolated rate is set to NaN.

    If dates is provided as a scalar and expirations as an array, the
    method assumes the scalar value is the same for all maturities. The same logic
    applies when the maturities are scalar and the trade dates are an array.

    Args:
        dates (DateScalar | DateArray): The trade dates for the rates.
        expirations (DateScalar | DateArray): The expirations corresponding to the
            trade dates.
        extrapolate (bool): Whether to allow extrapolation beyond known DI rates.

    Returns:
        pd.Series: A Series containing the interpolated DI rates.

    Raises:
        ValueError: If `dates` and `maturities` have different lengths.
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
    """Retrieve the DI rate for a specified expiration date."""
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
    Retorna as datas de negociação disponíveis no dataset Futuro de DI.

    Busca todas as datas únicas de `TradeDate` presentes no dataset "DI".

    Returns:
        pd.Series: Uma série de Timestamps contendo todas as datas de negociação
        (pregões) únicas, ordenadas ascendentemente, para as quais existem dados de DI.
        Retorna uma Series vazia se o cache estiver vazio.
    """
    return (
        get_cached_dataset("DI")
        .drop_duplicates(subset=["TradeDate"])["TradeDate"]
        .sort_values(ascending=True)
        .reset_index(drop=True)
    )
