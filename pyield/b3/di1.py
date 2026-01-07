import datetime as dt
import logging

import polars as pl

import pyield.converters as cv
from pyield import b3, bday, interpolator
from pyield.data_cache import get_cached_dataset
from pyield.types import ArrayLike, DateLike, has_nullable_args

logger = logging.getLogger(__name__)


def _load_with_intraday(dates: list[dt.date]) -> pl.DataFrame:
    """Busca dados de DI, incluindo dados intraday para datas ausentes no cache."""
    # 1. Busca inicial no cache com as datas solicitadas pelo usuário.
    df_cached = get_cached_dataset("di1").filter(pl.col("TradeDate").is_in(dates))

    # 2. Identifica datas solicitadas que não estão no cache
    requested_dates = set(dates)
    cached_dates = set(df_cached["TradeDate"].unique())
    missing_dates = requested_dates - cached_dates

    # 3. Para cada data faltante, tenta buscar dados via API
    dfs_to_concat = [df_cached] if not df_cached.is_empty() else []

    for ref_date in missing_dates:
        try:
            df_missing = b3.futures(contract_code="DI1", date=ref_date)

            # Só adiciona se tiver SettlementPrice
            if "SettlementPrice" in df_missing.columns:
                dfs_to_concat.append(df_missing)
            else:
                logger.warning(
                    f"Dados para {ref_date} não contêm 'SettlementPrice'. "
                    "Pulando esta data."
                )
        except Exception as e:
            logger.error(f"Falha ao buscar dados para {ref_date}: {e}")

    # 4. Retorna concatenação de todos os DataFrames disponíveis
    if len(dfs_to_concat) == 0:
        return pl.DataFrame()  # Retorna DataFrame vazio se nada foi encontrado
    elif len(dfs_to_concat) == 1:
        return dfs_to_concat[0]
    else:
        return pl.concat(dfs_to_concat, how="diagonal")


def _get_data(dates: DateLike | ArrayLike) -> pl.DataFrame:
    converted_dates = cv.convert_dates(dates)

    match converted_dates:
        case None:
            logger.warning("No valid dates provided. Returning empty DataFrame.")
            return pl.DataFrame()
        case dt.date():
            dates_list = [converted_dates]
        case pl.Series():
            dates_list = (
                pl.Series("TradeDate", converted_dates).unique().sort().to_list()
            )

    df = _load_with_intraday(dates_list)

    return df.sort(by=["TradeDate", "ExpirationDate"])


def data(
    dates: DateLike | ArrayLike,
    month_start: bool = False,
    pre_filter: bool = False,
) -> pl.DataFrame:
    """
    Retrieves DI Futures contract data for a specific trade date.

    Provides access to DI futures data, allowing adjustments to expiration dates
    (to month start) and optional filtering based on LTN and NTN-F bond maturities.

    Args:
        dates (DateLike): The trade dates for which to retrieve DI contract data.
        month_start (bool, optional): If True, adjusts all expiration dates to the
            first day of their respective month (e.g., 2025-02-01 becomes
            2025-01-01). Defaults to False.
        pre_filter (bool, optional): If True, filters DI contracts to include only
            those whose expiration dates match known prefixed Treasury bond (LTN, NTN-F)
            maturities from the TPF dataset nearest to the given trade date.
            Defaults to False.

    Returns:
        pl.DataFrame: A DataFrame containing the DI futures contract
            data for the specified dates, sorted by trade dates and expiration dates.
            Returns an empty DataFrame if no data is found

    Examples:
        >>> from pyield import di1
        >>> df = di1.data(dates="16-10-2024", month_start=True)
        >>> df
        shape: (38, 22)
        ┌────────────┬────────────────┬──────────────┬───────────┬───┬─────────┬───────────┬────────────────┬─────────────┐
        │ TradeDate  ┆ ExpirationDate ┆ TickerSymbol ┆ DaysToExp ┆ … ┆ MaxRate ┆ CloseRate ┆ SettlementRate ┆ ForwardRate │
        │ ---        ┆ ---            ┆ ---          ┆ ---       ┆   ┆ ---     ┆ ---       ┆ ---            ┆ ---         │
        │ date       ┆ date           ┆ str          ┆ i64       ┆   ┆ f64     ┆ f64       ┆ f64            ┆ f64         │
        ╞════════════╪════════════════╪══════════════╪═══════════╪═══╪═════════╪═══════════╪════════════════╪═════════════╡
        │ 2024-10-16 ┆ 2024-11-01     ┆ DI1X24       ┆ 16        ┆ … ┆ 0.10656 ┆ 0.10652   ┆ 0.10653        ┆ 0.10653     │
        │ 2024-10-16 ┆ 2024-12-01     ┆ DI1Z24       ┆ 47        ┆ … ┆ 0.10914 ┆ 0.10914   ┆ 0.1091         ┆ 0.110726    │
        │ 2024-10-16 ┆ 2025-01-01     ┆ DI1F25       ┆ 78        ┆ … ┆ 0.11174 ┆ 0.11164   ┆ 0.11164        ┆ 0.1154      │
        │ 2024-10-16 ┆ 2025-02-01     ┆ DI1G25       ┆ 110       ┆ … ┆ 0.1137  ┆ 0.11365   ┆ 0.11362        ┆ 0.118314    │
        │ 2024-10-16 ┆ 2025-03-01     ┆ DI1H25       ┆ 140       ┆ … ┆ 0.11595 ┆ 0.11565   ┆ 0.1157         ┆ 0.12343     │
        │ …          ┆ …              ┆ …            ┆ …         ┆ … ┆ …       ┆ …         ┆ …              ┆ …           │
        │ 2024-10-16 ┆ 2035-01-01     ┆ DI1F35       ┆ 3730      ┆ … ┆ 0.1267  ┆ 0.1264    ┆ 0.1265         ┆ 0.124455    │
        │ 2024-10-16 ┆ 2036-01-01     ┆ DI1F36       ┆ 4095      ┆ … ┆ null    ┆ null      ┆ 0.1263         ┆ 0.124249    │
        │ 2024-10-16 ┆ 2037-01-01     ┆ DI1F37       ┆ 4461      ┆ … ┆ null    ┆ null      ┆ 0.1263         ┆ 0.1263      │
        │ 2024-10-16 ┆ 2038-01-01     ┆ DI1F38       ┆ 4828      ┆ … ┆ null    ┆ null      ┆ 0.1263         ┆ 0.1263      │
        │ 2024-10-16 ┆ 2039-01-01     ┆ DI1F39       ┆ 5192      ┆ … ┆ null    ┆ null      ┆ 0.1263         ┆ 0.1263      │
        └────────────┴────────────────┴──────────────┴───────────┴───┴─────────┴───────────┴────────────────┴─────────────┘

    """  # noqa: E501
    if has_nullable_args(dates):
        logger.warning("No valid 'dates' provided. Returning empty DataFrame.")
        return pl.DataFrame()
    df = _get_data(dates=dates)

    if month_start:
        df = df.with_columns(pl.col("ExpirationDate").dt.truncate("1mo"))

    if pre_filter:
        df_pre = (
            get_cached_dataset("tpf")
            .filter(pl.col("BondType").is_in(["LTN", "NTN-F"]))
            .unique(subset=["ReferenceDate", "MaturityDate"])
            .select(
                TradeDate=pl.col("ReferenceDate"),
                ExpirationDate=pl.col("MaturityDate"),
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

    return df


def _build_input_dataframe(
    dates: DateLike | ArrayLike,
    expirations: DateLike | ArrayLike,
) -> pl.DataFrame:
    # 1. Converte as entradas primeiro
    converted_dates = cv.convert_dates(dates)
    converted_expirations = cv.convert_dates(expirations)

    # 2. Lida com os 4 casos de forma SIMPLES E LEGÍVEL
    match (converted_dates, converted_expirations):
        # CASO 1: Data escalar, vencimentos em array
        case dt.date() as d, pl.Series() as e:
            dfi = pl.DataFrame({"ExpirationDate": e}).with_columns(TradeDate=d)

        # CASO 2: Datas em array, vencimento escalar
        case pl.Series() as d, dt.date() as e:
            # Mesma lógica, invertida
            dfi = pl.DataFrame({"TradeDate": d}).with_columns(ExpirationDate=e)

        # CASO 3: Ambos são arrays
        case pl.Series() as d, pl.Series() as e:
            dfi = pl.DataFrame({"TradeDate": d, "ExpirationDate": e})

        # CASO 4: Ambos são escalares
        case dt.date() as d, dt.date() as e:
            dfi = pl.DataFrame({"TradeDate": [d], "ExpirationDate": [e]})

        # QUALQUER OUTRA COISA
        case _:
            dfi = pl.DataFrame()

    return dfi


def interpolate_rates(
    dates: DateLike | ArrayLike,
    expirations: DateLike | ArrayLike,
    extrapolate: bool = True,
) -> pl.Series:
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
        dates (DateLike | ArrayLike): The trade date(s) for the rates.
        expirations (DateLike | ArrayLike): The corresponding expiration date(s).
            Must be compatible in length with `dates` if both are arrays.
        extrapolate (bool, optional): Whether to allow extrapolation beyond the
            range of known DI rates for a given trade date. Defaults to True.

    Returns:
        pl.Series: A Series containing the interpolated DI rates (as floats).
            Values will be NaN where interpolation is not possible
            (e.g., no DI data for the trade date).

    Examples:
        - Interpolate rates for multiple trade and expiration dates
        >>> # For contract with expiration 01-01-2027 in 08-05-2025
        >>> # The rate is not interpolated (settlement rate is used)
        >>> # There is no contract with expiration 25-11-2027 in 09-05-2025
        >>> # The rate is interpolated (flat-forward method)
        >>> # There is no data for trade date 10-05-2025 (Saturday) -> NaN
        >>> # Note: 0.13461282461562996 is shown as 0.134613
        >>> from pyield import di1
        >>> di1.interpolate_rates(
        ...     dates=["08-05-2025", "09-05-2025", "10-05-2025"],
        ...     expirations=["01-01-2027", "25-11-2027", "01-01-2030"],
        ... )
        shape: (3,)
        Series: 'FlatFwdRate' [f64]
        [
            0.13972
            0.134613
            null
        ]

        - Interpolate rates for a single trade date and multiple expiration dates
        >>> # There is no DI Contract in 09-05-2025 with expiration 01-01-2050
        >>> # The longest available contract is used to extrapolate the rate
        >>> # Note: extrapolation is allowed by default
        >>> di1.interpolate_rates(
        ...     dates="25-04-2025",
        ...     expirations=["01-01-2027", "01-01-2050"],
        ... )
        shape: (2,)
        Series: 'FlatFwdRate' [f64]
        [
            0.13901
            0.13881
        ]

        >>> # With extrapolation set to False, the second rate will be null
        >>> # Note: 0.13576348733268917 is shown as 0.135763
        >>> di1.interpolate_rates(
        ...     dates="25-04-2025",
        ...     expirations=["01-11-2027", "01-01-2050"],
        ...     extrapolate=False,
        ... )
        shape: (2,)
        Series: 'FlatFwdRate' [f64]
        [
            0.135763
            null
        ]

    Raises:
        ValueError: If `dates` and `expirations` are both array-like but have
            different lengths.

    Notes:
        - All available settlement rates are used for the flat-forward interpolation.
        - The function handles broadcasting of scalar and array-like inputs.
    """
    if has_nullable_args(dates, expirations):
        logger.warning(
            "Both 'dates' and 'expirations' must be provided. Returning empty Series."
        )
        return pl.Series(dtype=pl.Float64)

    dfi = _build_input_dataframe(dates, expirations)
    # 2. Se a helper retornou um DataFrame vazio, retornar uma Series vazia
    if dfi.is_empty():
        logger.warning("Invalid inputs provided. Returning empty Series.")
        return pl.Series(dtype=pl.Float64)

    s_bdays = bday.count(dfi["TradeDate"], dfi["ExpirationDate"])

    # Inicializa FlatFwdRate como None
    dfi = dfi.with_columns(BDaysToExp=s_bdays, FlatFwdRate=None)

    # Load DI rates dataset filtered by the provided reference dates
    dfr = _get_data(dates=dates)

    # Return an empty DataFrame if no rates are found
    if dfr.is_empty():
        return pl.Series(dtype=pl.Float64)

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
            .then(pl.col("BDaysToExp").map_elements(interp, return_dtype=pl.Float64))
            .otherwise(pl.col("FlatFwdRate"))
            .alias("FlatFwdRate")
        )

    # Return the interpolated rates with nulls where interpolation was not possible
    return dfi.get_column("FlatFwdRate").fill_nan(None)


def interpolate_rate(
    date: DateLike,
    expiration: DateLike,
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
        date (DateLike): The trade date for which to retrieve DI data.
        expiration (DateLike): The target expiration date for the rate.
        extrapolate (bool, optional): If True, allows extrapolation if the
            `expiration` date falls outside the range of available contract
            expirations for the given `date`. Defaults to False.

    Returns:
        float: The exact or interpolated DI settlement rate for the specified
            date and expiration. Returns `float("nan")` if:
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
    if has_nullable_args(date, expiration):
        logger.warning("Both 'date' and 'expiration' must be provided. Returning NaN.")
        return float("nan")

    converted_date = cv.convert_dates(date)
    converted_expiration = cv.convert_dates(expiration)

    if not isinstance(converted_date, dt.date) or not isinstance(
        converted_expiration, dt.date
    ):
        raise ValueError("Both 'date' and 'expiration' must be single date values.")

    # Get the DI contract DataFrame
    df = _get_data(dates=converted_date)

    if df.is_empty():
        return float("nan")

    ff_interp = interpolator.Interpolator(
        method="flat_forward",
        known_bdays=df["BDaysToExp"],
        known_rates=df["SettlementRate"],
        extrapolate=extrapolate,
    )

    bd = bday.count(converted_date, converted_expiration)
    return ff_interp(bd)


def available_trade_dates() -> pl.Series:
    """
    Returns all available (completed) trading dates in the DI dataset.

    Retrieves distinct 'TradeDate' values present in the
    historical DI futures data cache, sorted chronologically.

    Returns:
        pl.Series: A sorted Series of unique trade dates (dt.date)
            for which DI data is available.

    Examples:
        >>> from pyield import di1
        >>> # DI Futures series starts from 1995-01-02
        >>> di1.available_trade_dates().head(5)
        shape: (5,)
        Series: 'available_dates' [date]
        [
            1995-01-02
            1995-01-03
            1995-01-04
            1995-01-05
            1995-01-06
        ]
    """
    available_dates = (
        get_cached_dataset("di1")
        .get_column("TradeDate")
        .unique()
        .sort()
        .alias("available_dates")
    )
    return available_dates
