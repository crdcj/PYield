import datetime as dt
import logging

import pandas as pd
import polars as pl

import pyield.date_converter as dc
from pyield import b3, bday, interpolator
from pyield.config import TIMEZONE_BZ
from pyield.data_cache import get_cached_dataset
from pyield.date_converter import DateArray, DateScalar

logger = logging.getLogger(__name__)


def _load_with_intraday(dates: list[dt.date]) -> pl.DataFrame:
    """Busca dados de DI, incluindo dados intraday para o dia corrente se necessário."""
    # 1. Busca inicial no cache com as datas solicitadas pelo usuário.
    df_cached = get_cached_dataset("di1").filter(pl.col("TradeDate").is_in(dates))

    today = dt.datetime.now(TIMEZONE_BZ).date()
    last_bday = bday.last_business_day()

    # 2. Lógica para buscar dados intraday.
    #    Isso é necessário quando o usuário solicita os dados do dia corrente
    #    e eles ainda não foram persistidos no cache (processo noturno).
    # Condição 1: O dia de hoje foi solicitado.
    has_today = today in dates
    # Condição 2: E ainda não está no cache.
    is_today_not_in_cache = df_cached.filter(pl.col("TradeDate") == today).is_empty()
    # Condição 3: E hoje é o último dia útil.
    is_today_last_bday = today == last_bday

    if has_today and is_today_not_in_cache and is_today_last_bday:
        try:
            df_intraday = b3.futures(contract_code="DI1", date=today)
            df_intraday = pl.from_pandas(df_intraday)
            if "SettlementPrice" not in df_intraday.columns or df_intraday.is_empty():
                logger.warning(
                    f"Ainda sem dados de ajustes intraday para {today}."
                    " Retornando apenas dados do cache."
                )
                df_intraday = df_intraday.drop("DaysToExp", strict=False)

                return df_cached
            if df_cached.is_empty():
                return df_intraday
            return pl.concat([df_cached, df_intraday], how="diagonal")
        except Exception as e:
            logger.error(f"Falha ao buscar dados intraday para {today}: {e}")

    # 3. Se a lógica intraday não for acionada ou falhar, retorna apenas dados do cache.
    return df_cached


def _get_data(dates: DateScalar | DateArray) -> pl.DataFrame:
    converted_dates = dc.convert_input_dates(dates)

    match converted_dates:
        case None:
            logger.warning("No valid dates provided. Returning empty DataFrame.")
            return pl.DataFrame()
        case dt.date():
            dates_list = [converted_dates]
        case pd.Series():
            dates_list = (
                pl.Series("TradeDate", converted_dates).unique().sort().to_list()
            )

    df = _load_with_intraday(dates_list)

    return df.sort(by=["TradeDate", "ExpirationDate"])


def data(
    dates: DateScalar | DateArray,
    month_start: bool = False,
    pre_filter: bool = False,
    all_columns: bool = True,
) -> pd.DataFrame:
    """
    Retrieves DI Futures contract data for a specific trade date.

    Provides access to DI futures data, allowing adjustments to expiration dates
    (to month start) and optional filtering based on LTN and NTN-F bond maturities.

    Args:
        dates (DateScalar): The trade dates for which to retrieve DI contract data.
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
            specified dates, sorted by trade dates and expiration dates.
            Returns an empty DataFrame if no data is found

    Examples:
        >>> from pyield import di1
        >>> df = di1.data(dates="16-10-2024", month_start=True)
        >>> df.iloc[:5, :5]  # Show the first five rows and columns
           TradeDate ExpirationDate TickerSymbol  BDaysToExp  OpenContracts
        0 2024-10-16     2024-11-01       DI1X24          12        1744269
        1 2024-10-16     2024-12-01       DI1Z24          31        1429375
        2 2024-10-16     2025-01-01       DI1F25          52        5423969
        3 2024-10-16     2025-02-01       DI1G25          74         279491
        4 2024-10-16     2025-03-01       DI1H25          94         344056
    """
    df = _get_data(dates=dates)

    if month_start:
        df = df.with_columns(pl.col("ExpirationDate").dt.truncate("1mo"))

    if pre_filter:
        df_pre = (
            get_cached_dataset("tpf")
            .filter(pl.col("BondType").is_in(["LTN", "NTN-F"]))
            .unique(subset=["ReferenceDate", "MaturityDate"])
            .select(
                pl.col("ReferenceDate").alias("TradeDate"),
                pl.col("MaturityDate").alias("ExpirationDate"),
            )
        )

        # garante que os dois lados estão ordenados pelas chaves necessárias
        df = df.sort(["TradeDate", "ExpirationDate"])
        df_pre = df_pre.sort(["TradeDate", "ExpirationDate"])

        df = df.join_asof(
            df_pre,
            left_on="TradeDate",
            right_on="TradeDate",
            by="ExpirationDate",  # garante matching por vértice
            strategy="backward",  # pega a data anterior se não tiver exata
            check_sortedness=False,  # já garantimos a ordenação
        )

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
        df = df.select(selected_cols)

    return df.to_pandas(use_pyarrow_extension_array=True)


def _build_input_dataframe(
    dates: DateScalar | DateArray,
    expirations: DateScalar | DateArray,
) -> pl.DataFrame:
    # 1. Converte as entradas primeiro
    converted_dates = dc.convert_input_dates(dates)
    converted_expirations = dc.convert_input_dates(expirations)

    # 2. Lida com os 4 casos de forma SIMPLES E LEGÍVEL
    match (converted_dates, converted_expirations):
        # CASO 1: Data escalar, vencimentos em array
        case dt.date() as d, pd.Series() as e:
            if e.empty:
                dfi = pl.DataFrame()
            else:
                # Cria o DF com o array, e ADICIONA o escalar com pl.lit()
                dfi = pl.DataFrame({"ExpirationDate": e}).with_columns(TradeDate=d)

        # CASO 2: Datas em array, vencimento escalar
        case pd.Series() as d, dt.date() as e:
            if d.empty:
                dfi = pl.DataFrame()
            else:
                # Mesma lógica, invertida
                dfi = pl.DataFrame({"TradeDate": d}).with_columns(ExpirationDate=e)

        # CASO 3: Ambos são arrays
        case pd.Series() as d, pd.Series() as e:
            if d.empty or e.empty:
                dfi = pl.DataFrame()
            elif len(d) != len(e):
                raise ValueError("'dates' e 'expirations' devem ter o mesmo tamanho.")
            else:
                dfi = pl.DataFrame({"TradeDate": d, "ExpirationDate": e})

        # CASO 4: Ambos são escalares
        case dt.date() as d, dt.date() as e:
            dfi = pl.DataFrame({"TradeDate": [d], "ExpirationDate": [e]})

        # QUALQUER OUTRA COISA
        case _:
            dfi = pl.DataFrame()

    return dfi


def interpolate_rates(
    dates: DateScalar | DateArray | None,
    expirations: DateScalar | DateArray | None,
    extrapolate: bool = True,
) -> pd.Series:
    """
    Interpolates DI rates for specified trade dates and expiration dates.

    Calculates interpolated DI rates using the **flat-forward** method for given
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

    Examples:
        >>> from pyield import di1
        >>> # Note: by default, pandas shows floats with 6 decimal places
        >>> # Interpolate rates for multiple trade and expiration dates
        >>> # There is a contract with expiration 01-01-2027 in 08-05-2025
        >>> # The rate is not interpolated (settlement rate is used)
        >>> # There is no contract with expiration 25-11-2027 in 09-05-2025
        >>> # The rate is interpolated (flat-forward method)
        >>> # There is no data for trade date 10-05-2025 (Saturday) -> NaN
        >>> # Note: 0.13461282461562996 is shown as 0.134613
        >>> di1.interpolate_rates(
        ...     dates=["08-05-2025", "09-05-2025", "10-05-2025"],
        ...     expirations=["01-01-2027", "25-11-2027", "01-01-2030"],
        ... )
        0    0.13972
        1    0.134613
        2    <NA>
        Name: irate, dtype: double[pyarrow]

        >>> # Interpolate rates for a single trade date and multiple expiration dates
        >>> # There is no DI Contract in 09-05-2025 with expiration 01-01-2050
        >>> # The longest available contract is used to extrapolate the rate
        >>> # Note: extrapolation is allowed by default
        >>> di1.interpolate_rates(
        ...     dates="25-04-2025",
        ...     expirations=["01-01-2027", "01-01-2050"],
        ... )
        0    0.13901
        1    0.13881
        Name: irate, dtype: double[pyarrow]

        >>> # With extrapolation set to False, the second rate will be NaN
        >>> # Note: 0.13576348733268917 is shown as 0.135763
        >>> di1.interpolate_rates(
        ...     dates="25-04-2025",
        ...     expirations=["01-11-2027", "01-01-2050"],
        ...     extrapolate=False,
        ... )
        0    0.135763
        1    NaN
        Name: irate, dtype: double[pyarrow]

    Notes:
        - All available settlement rates are used for the flat-forward interpolation.
        - The function handles broadcasting of scalar and array-like inputs.
    """
    dfi = _build_input_dataframe(dates, expirations)

    # 2. Se a helper retornou None, a entrada é inválida.
    if dfi.is_empty():
        logger.warning("Invalid or empty dates provided. Returning empty Series.")
        return pd.Series(dtype="float64[pyarrow]")

    # bday.count retorna uma Series de inteiros do pandas
    s_bdays = bday.count(dfi["TradeDate"], dfi["ExpirationDate"])
    dfi = dfi.with_columns(pl.Series("bdays", s_bdays), irate=None)

    # Load DI rates dataset filtered by the provided reference dates
    dfr = _get_data(dates=dates)

    # Return an empty DataFrame if no rates are found
    if dfr.is_empty():
        return pd.Series(dtype="float64[pyarrow]")

    # Iterate over each unique reference date
    for date in dfi.get_column("TradeDate").unique().to_list():
        # Filter DI rates for the current reference date
        dfr_subset = dfr.filter(pl.col("TradeDate") == date)

        # Skip processing if no rates are available for the current date
        if dfr_subset.is_empty():
            continue

        # Initialize the interpolator with known rates and business days
        interp = interpolator.Interpolator(
            method="flat_forward",
            known_bdays=dfr_subset["BDaysToExp"],
            known_rates=dfr_subset["SettlementRate"],
            extrapolate=extrapolate,
        )

        dfi = dfi.with_columns(
            pl.when(pl.col("TradeDate") == date)
            .then(pl.col("bdays").map_elements(interp, return_dtype=pl.Float64))
            .otherwise(pl.col("irate"))
            .alias("irate")
        )

    # Return the Series with interpolated rates
    irates = dfi.get_column("irate")
    return irates.to_pandas(use_pyarrow_extension_array=True)


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
        >>> from pyield import di1
        >>> # Get rate for an existing contract expiration
        >>> di1.interpolate_rate("25-04-2025", "01-01-2027")
        0.13901

        >>> # Get rate for a non-existing contract expiration
        >>> di1.interpolate_rate("25-04-2025", "01-11-2027")
        0.13576348733268917

        >>> # Extrapolate rate for a future expiration date
        >>> di1.interpolate_rate("25-04-2025", "01-01-2050", extrapolate=True)
        0.13881
    """
    converted_date = dc.convert_input_dates(date)
    converted_expiration = dc.convert_input_dates(expiration)

    if not isinstance(converted_date, dt.date) or not isinstance(
        converted_expiration, dt.date
    ):
        raise ValueError("Both 'date' and 'expiration' must be single date values.")

    if not converted_date or not converted_expiration:
        return float("nan")

    # Get the DI contract DataFrame
    df = _get_data(dates=converted_date)

    if df.is_empty():
        return float("nan")

    max_exp = df.get_column("ExpirationDate").max()

    if converted_expiration > max_exp and not extrapolate:
        logger.warning(
            f"Expiration ({converted_expiration}) is greater than the maximum exp. "
            f"date ({max_exp}) and extrapolation is not allowed. Returning NaN."
        )
        return float("nan")

    rate = df.filter(pl.col("ExpirationDate") == converted_expiration).get_column(
        "SettlementRate"
    )

    if not rate.is_empty():
        logger.info(f"Exact match found for expiration {converted_expiration}.")
        return rate.item(0)

    ff_interp = interpolator.Interpolator(
        method="flat_forward",
        known_bdays=df.get_column("BDaysToExp"),
        known_rates=df.get_column("SettlementRate"),
        extrapolate=extrapolate,
    )
    bd = bday.count(converted_date, converted_expiration)
    if not bd:
        return float("nan")

    return ff_interp(bd)


def available_trade_dates() -> pd.Series:
    """
    Returns all available (completed) trading dates in the DI dataset.

    Retrieves distinct 'TradeDate' values present in the
    historical DI futures data cache, sorted chronologically.

    Returns:
        pd.Series: A sorted Series of unique trade dates (dt.date)
                   for which DI data is available.

    Examples:
        >>> from pyield import di1
        >>> # DI Futures series starts from 1995-01-02
        >>> di1.available_trade_dates().head(5)
        0   1995-01-02
        1   1995-01-03
        2   1995-01-04
        3   1995-01-05
        4   1995-01-06
        Name: available_dates, dtype: date32[day][pyarrow]
    """
    available_dates = (
        get_cached_dataset("di1")
        .unique(subset=["TradeDate"])
        .get_column("TradeDate")
        .sort(descending=False)
        .alias("available_dates")
        .to_pandas(use_pyarrow_extension_array=True)
    )
    return available_dates
