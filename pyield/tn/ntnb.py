import datetime as dt
import logging

import pandas as pd
import polars as pl

import pyield.converters as cv
import pyield.interpolator as ip
import pyield.tn.tools as tl
from pyield import anbima, bday, fwd
from pyield.types import ArrayLike, DateLike, has_nullable_args

"""
Constants calculated as per Anbima Rules and in base 100
COUPON_RATE = (0.06 + 1) ** 0.5 - 1  # 6% annual rate compounded semi-annually
COUPON_PMT = round(100 * COUPON_RATE, 6) -> 2.956301
FINAL_PMT = principal + last coupon payment = 100 + 2.956301
COUPON_DAY = 15
COUPON_MONTHS = {2, 5, 8, 11}
"""
COUPON_PMT = 2.956301
FINAL_PMT = 102.956301

logger = logging.getLogger(__name__)


def data(date: DateLike) -> pl.DataFrame:
    """
    Fetch the bond indicative rates for the given reference date.

    Args:
        date (DateLike): The reference date for fetching the data.

    Returns:
        pl.DataFrame: DataFrame with columns "MaturityDate" and "IndicativeRate".

    Returned columns:
        - ReferenceDate: The reference date of the data.
        - BondType: The type of the bond (NTN-B).
        - SelicCode: The SELIC code of the bond.
        - IssueBaseDate: The issue base date of the bond.
        - MaturityDate: The maturity date of the bond.
        - BDToMat: The number of business days to maturity.
        - Duration: The duration of the bond.
        - DV01: The DV01 of the bond.
        - DV01USD: The DV01 in USD of the bond.
        - Price: The price of the bond.
        - BidRate: The bid rate of the bond.
        - AskRate: The ask rate of the bond.
        - IndicativeRate: The indicative rate for the bond.
        - DIRate: The interpolated DI rate for the bond.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.data("23-08-2024")
        shape: (14, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬──────────┬──────────┬────────────────┬──────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate  ┆ AskRate  ┆ IndicativeRate ┆ DIRate   │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---      ┆ ---      ┆ ---            ┆ ---      │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64      ┆ f64      ┆ f64            ┆ f64      │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪══════════╪══════════╪════════════════╪══════════╡
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.063961 ┆ 0.063667 ┆ 0.063804       ┆ 0.112749 │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.06594  ┆ 0.065635 ┆ 0.065795       ┆ 0.114963 │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.063925 ┆ 0.063601 ┆ 0.063794       ┆ 0.114888 │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.063217 ┆ 0.062905 ┆ 0.063094       ┆ 0.115595 │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.062245 ┆ 0.061954 ┆ 0.0621         ┆ 0.115665 │
        │ …             ┆ …        ┆ …         ┆ …             ┆ … ┆ …        ┆ …        ┆ …              ┆ …        │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.060005 ┆ 0.059574 ┆ 0.059797       ┆ 0.11511  │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.061107 ┆ 0.060733 ┆ 0.060923       ┆ 0.11511  │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.061304 ┆ 0.060931 ┆ 0.06114        ┆ 0.11511  │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.061053 ┆ 0.06074  ┆ 0.060892       ┆ 0.11511  │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.061211 ┆ 0.0608   ┆ 0.061005       ┆ 0.11511  │
        └───────────────┴──────────┴───────────┴───────────────┴───┴──────────┴──────────┴────────────────┴──────────┘
    """  # noqa: E501
    return anbima.tpf_data(date, "NTN-B")


def maturities(date: DateLike) -> pl.Series:
    """
    Get the bond maturities available for the given reference date.

    Args:
        date (DateLike): The reference date for fetching the data.

    Returns:
        pl.Series: Series containing the maturity dates for the NTN-B bonds.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.maturities("16-08-2024")
        shape: (14,)
        Series: 'MaturityDate' [date]
        [
            2025-05-15
            2026-08-15
            2027-05-15
            2028-08-15
            2029-05-15
            …
            2040-08-15
            2045-05-15
            2050-08-15
            2055-05-15
            2060-08-15
        ]
    """
    return data(date)["MaturityDate"]


def _generate_all_coupon_dates(
    start: DateLike,
    end: DateLike,
) -> pl.Series:
    """
    Generate a map of all possible coupon dates between the start and end dates.
    The dates are inclusive. Coupon payments are made on the 15th of February, May,
    August, and November (15-02, 15-05, 15-08, and 15-11 of each year).

    Args:
        start (DateLike): The start date.
        end (DateLike): The end date.

    Returns:
        pl.Series: Series of coupon dates within the specified range.
    """
    start = cv.convert_dates(start)
    end = cv.convert_dates(end)

    first_coupon_date = dt.date(start.year, 2, 1)

    # Generate coupon dates on the 1st of the month
    coupon_dates: pl.Series = pl.date_range(
        start=first_coupon_date, end=end, interval="3mo", eager=True
    )
    # Offset dates to the 15th
    coupon_dates = coupon_dates.dt.offset_by("14d")

    # First coupon date must be after the start date
    return coupon_dates.filter(coupon_dates > start)


def payment_dates(
    settlement: DateLike,
    maturity: DateLike,
) -> pl.Series:
    """
    Generate all remaining coupon dates between a given date and the maturity date.
    The dates are inclusive. Coupon payments are made on the 15th of February, May,
    August, and November (15-02, 15-05, 15-08, and 15-11 of each year). The NTN-B
    bond is determined by its maturity date.

    Args:
        settlement (DateLike): The settlement date (exlusive) to start generating
            the coupon dates.
        maturity (DateLike): The maturity date.

    Returns:
        pl.Series: Series of coupon dates within the specified range. Returns an empty
            Series if the maturity date is before or equal to the settlement date.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.payment_dates("10-05-2024", "15-05-2025")
        shape: (3,)
        Series: 'payment_dates' [date]
        [
            2024-05-15
            2024-11-15
            2025-05-15
        ]
    """
    if has_nullable_args(settlement, maturity):
        return pl.Series(dtype=pl.Date)

    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    if maturity <= settlement:
        return pl.Series(dtype=pl.Date)

    coupon_date = maturity
    coupon_dates = []

    while coupon_date > settlement:
        coupon_dates.append(coupon_date)
        coupon_date -= pd.DateOffset(months=6)
        coupon_date = coupon_date.date()

    return pl.Series(name="payment_dates", values=coupon_dates).sort()


def cash_flows(
    settlement: DateLike,
    maturity: DateLike,
) -> pl.DataFrame:
    """
    Generate the cash flows for NTN-B bonds between the settlement and maturity dates.

    Args:
        settlement (DateLike): The settlement date (exclusive) to start generating
            the cash flows.
        maturity (DateLike): The maturity date of the bond.

    Returns:
        pl.DataFrame: DataFrame with columns "PaymentDate" and "CashFlow".
            Returns empty DataFrame if settlement >= maturity.

    Returned columns:
        - PaymentDate: The payment date of the cash flow
        - CashFlow: Cash flow value for the bond

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.cash_flows("10-05-2024", "15-05-2025")
        shape: (3, 2)
        ┌─────────────┬────────────┐
        │ PaymentDate ┆ CashFlow   │
        │ ---         ┆ ---        │
        │ date        ┆ f64        │
        ╞═════════════╪════════════╡
        │ 2024-05-15  ┆ 2.956301   │
        │ 2024-11-15  ┆ 2.956301   │
        │ 2025-05-15  ┆ 102.956301 │
        └─────────────┴────────────┘
    """
    if has_nullable_args(settlement, maturity):
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    # Get the coupon dates between the settlement and maturity dates
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)
    p_dates = payment_dates(settlement, maturity)

    # Return empty DataFrame if no payment dates (settlement >= maturity)
    if p_dates.is_empty():
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    df = pl.DataFrame(
        {"PaymentDate": p_dates},
    ).with_columns(
        pl.when(pl.col("PaymentDate") == maturity)
        .then(FINAL_PMT)
        .otherwise(COUPON_PMT)
        .alias("CashFlow")
    )

    return df


def quotation(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calculate the NTN-B quotation in base 100 using Anbima rules.

    Args:
        settlement (DateLike): The settlement date of the operation.
        maturity (DateLike): The maturity date of the NTN-B bond.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-B.

    Returns:
        float: The NTN-B quotation truncated to 4 decimal places. Returns float('nan')
            if the calculation cannot be performed due to invalid inputs.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - The semi-annual coupon is set to 2.956301, which represents a 6% annual
          coupon rate compounded semi-annually and rounded to 6 decimal places as per
          Anbima rules.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)
        99.3651
        >>> ntnb.quotation("31-05-2024", "15-08-2060", 0.061878)
        99.5341
        >>> ntnb.quotation("15-08-2024", "15-08-2032", 0.05929)
        100.6409
    """
    if has_nullable_args(settlement, maturity, rate):
        return float("nan")

    df_cf = cash_flows(settlement, maturity)
    if df_cf.is_empty():
        return float("nan")

    cf_dates = df_cf["PaymentDate"]
    cf_values = df_cf["CashFlow"]

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement, cf_dates)

    # Calculate the number of periods truncated as per Anbima rules
    byears = tl.truncate(bdays / 252, 14)

    discount_factor = (1 + rate) ** byears

    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    cf_present_value = (cf_values / discount_factor).round(10)

    # Return the quotation (the dcf sum) truncated as per Anbima rules
    return tl.truncate(cf_present_value.sum(), 4)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calculate the NTN-B price using Anbima rules.

    Args:
        vna (float): The nominal value of the NTN-B bond.
        quotation (float): The NTN-B quotation in base 100.

    Returns:
        float: The NTN-B price truncated to 6 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.price(4299.160173, 99.3651)
        4271.864805
        >>> ntnb.price(4315.498383, 100.6409)
        4343.156412
        >>> ntnb.price(None, 99.5341)  # Nullable inputs return float('nan')
        nan
    """
    if has_nullable_args(vna, quotation):
        return float("nan")
    return tl.truncate(vna * quotation / 100, 6)


def _validate_spot_rate_inputs(
    settlement: DateLike,
    maturities: ArrayLike,
    rates: ArrayLike,
) -> tuple[dt.date, pl.Series, pl.Series]:
    # Process and validate the input data
    settlement = cv.convert_dates(settlement)
    maturities = cv.convert_dates(maturities)

    # Structural validation: ensure maturities and rates have the same length
    if len(maturities) != len(rates):
        raise ValueError(
            f"Maturities and rates must have the same length. "
            f"Got {len(maturities)} maturities and {len(rates)} rates."
        )

    # Create base dataframe and filter out invalid data
    df_clean = pl.DataFrame(
        data={"maturities": maturities, "rates": rates},
        schema={"maturities": pl.Date, "rates": pl.Float64},
    ).filter(pl.col("maturities") > settlement)

    # Warn about filtered maturities
    filtered_count = len(maturities) - df_clean.height
    if filtered_count > 0:
        logger.warning(
            "Maturities on or before settlement were ignored: "
            f"{filtered_count} entries removed."
        )

    return settlement, df_clean["maturities"], df_clean["rates"]


def _create_bootstrap_dataframe(
    settlement: dt.date,
    rates: pl.Series,
    maturities: pl.Series,
) -> pl.DataFrame:
    """Cria o DataFrame base para o bootstrap."""
    # Create the interpolator to calculate the YTM rates for intermediate dates
    flat_fwd = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, maturities),
        known_rates=rates,
    )

    # Generate coupon dates up to the longest maturity date
    all_coupon_dates = _generate_all_coupon_dates(settlement, maturities.max())
    bdays_to_mat = bday.count(settlement, all_coupon_dates)
    ytm_rates = flat_fwd.interpolate(bdays_to_mat)

    df = (
        pl.DataFrame(
            {
                "MaturityDate": all_coupon_dates,
                "BDToMat": bdays_to_mat,
                "BYears": bdays_to_mat / 252,
                "YTM": ytm_rates,
            }
        )
        .with_columns(
            Coupon=pl.lit(COUPON_PMT),
            SpotRate=pl.lit(None, dtype=pl.Float64),
        )
        .sort("MaturityDate")
    )
    return df


def _update_spot_rate(
    df: pl.DataFrame, maturity: dt.date, spot_rate: float
) -> pl.DataFrame:
    """Helper function to update the spot rate inside the bootstrap loop."""
    return df.with_columns(
        pl.when(pl.col("MaturityDate") == maturity)
        .then(spot_rate)
        .otherwise(pl.col("SpotRate"))
        .alias("SpotRate")
    )


def _calculate_coupons_present_value(
    df: pl.DataFrame,
    settlement: dt.date,
    maturity: dt.date,
) -> float:
    """Calcula o valor presente dos cupons anteriores à maturidade."""
    prev_cf_dates = payment_dates(settlement, maturity).to_list()[:-1]
    df_temp = df.filter(pl.col("MaturityDate").is_in(prev_cf_dates))

    return tl.calculate_present_value(
        df_temp["Coupon"],
        df_temp["SpotRate"],
        df_temp["BYears"],
    )


def spot_rates(
    settlement: DateLike,
    maturities: ArrayLike,
    rates: ArrayLike,
    show_coupons: bool = False,
) -> pl.DataFrame:
    """
    Calculate the spot rates for NTN-B bonds using the bootstrap method.

    The bootstrap method is a process used to determine spot rates from
    the yields of a series of bonds. It involves iteratively solving for
    the spot rates that discount each bond's cash flows to its current
    price.

    Args:
        settlement (DateLike): The reference date for settlement.
        maturities (ArrayLike): Series of maturity dates for the bonds.
        rates (ArrayLike): Series of yield to maturity rates.
        show_coupons (bool, optional): If True, the result will include the
            intermediate coupon dates. Defaults to False.

    Returns:
        pl.DataFrame: DataFrame with columns "MaturityDate", "SpotRate".

    Examples:
        >>> from pyield import ntnb
        >>> # Get the NTN-B rates for a specific reference date
        >>> df = ntnb.data("16-08-2024")
        >>> # Calculate the spot rates considering the settlement at the reference date
        >>> ntnb.spot_rates(
        ...     settlement="16-08-2024",
        ...     maturities=df["MaturityDate"],
        ...     rates=df["IndicativeRate"],
        ... )
        shape: (14, 3)
        ┌──────────────┬─────────┬──────────┐
        │ MaturityDate ┆ BDToMat ┆ SpotRate │
        │ ---          ┆ ---     ┆ ---      │
        │ date         ┆ i64     ┆ f64      │
        ╞══════════════╪═════════╪══════════╡
        │ 2025-05-15   ┆ 185     ┆ 0.063893 │
        │ 2026-08-15   ┆ 502     ┆ 0.066141 │
        │ 2027-05-15   ┆ 687     ┆ 0.064087 │
        │ 2028-08-15   ┆ 1002    ┆ 0.063057 │
        │ 2029-05-15   ┆ 1186    ┆ 0.061458 │
        │ …            ┆ …       ┆ …        │
        │ 2040-08-15   ┆ 4009    ┆ 0.058326 │
        │ 2045-05-15   ┆ 5196    ┆ 0.060371 │
        │ 2050-08-15   ┆ 6511    ┆ 0.060772 │
        │ 2055-05-15   ┆ 7700    ┆ 0.059909 │
        │ 2060-08-15   ┆ 9017    ┆ 0.060652 │
        └──────────────┴─────────┴──────────┘

    Notes:
        The calculation of the spot rates for NTN-B bonds considers the following steps:
            - Map all all possible payment dates up to the longest maturity date.
            - Interpolate the YTM rates in the intermediate payment dates.
            - Calculate the NTN-B quotation for each maturity date.
            - Calculate the real spot rates for each maturity date.

    Columns returned:
        - MaturityDate: The maturity date of the bond.
        - BDToMat: The number of business days from settlement to maturities.
        - SpotRate: The real spot rate for the bond.
    """
    if has_nullable_args(settlement, maturities, rates):
        return pl.DataFrame()

    settlement, maturities, rates = _validate_spot_rate_inputs(
        settlement, maturities, rates
    )

    df = _create_bootstrap_dataframe(settlement, rates, maturities)

    # Bootstrap method to calculate spot rates iteratively
    df_dicts = df.to_dicts()
    first_maturity = maturities.min()
    for row in df_dicts:
        maturity = row["MaturityDate"]

        # Spot rates <= first maturity are YTM rates by definition
        if maturity <= first_maturity:
            spot_rate = row["YTM"]
            df = _update_spot_rate(df, maturity, spot_rate)
            continue

        # Calculate the spot rate for the current maturity
        coupons_pv = _calculate_coupons_present_value(df, settlement, maturity)
        bond_price = quotation(settlement, maturity, row["YTM"])
        price_factor = FINAL_PMT / (bond_price - coupons_pv)
        spot_rate = price_factor ** (1 / row["BYears"]) - 1

        df = _update_spot_rate(df, maturity, spot_rate)

    if not show_coupons:
        df = df.filter(pl.col("MaturityDate").is_in(maturities.to_list()))
    return df.select(["MaturityDate", "BDToMat", "SpotRate"])


def bei_rates(
    settlement: DateLike,
    ntnb_maturities: ArrayLike,
    ntnb_rates: ArrayLike,
    nominal_maturities: ArrayLike,
    nominal_rates: ArrayLike,
) -> pl.DataFrame:
    """
    Calculate the Breakeven Inflation (BEI) for NTN-B bonds based on nominal and real
    interest rates. The BEI represents the inflation rate that equalizes the real and
    nominal yields. The calculation is based on the spot rates for NTN-B bonds.

    Args:
        settlement (DateLike): The settlement date of the operation.
        ntnb_maturities (ArrayLike): The maturity dates for the NTN-B bonds.
        ntnb_rates (ArrayLike): The real interest rates (Yield to Maturity - YTM)
            corresponding to the given NTN-B maturities.
        nominal_maturities (ArrayLike): The maturity dates to be used as reference for
            nominal rates.
        nominal_rates (ArrayLike): The nominal interest rates (e.g. DI Futures or
             zero prefixed bonds rates) used as reference for the calculation.

    Returns:
        pl.DataFrame: DataFrame containing the calculated breakeven inflation rates.

    Returned columns:
        - MaturityDate: The maturity date of the bonds.
        - BDToMat: The number of business days from the settlement to the maturity.
        - RIR: The calculated Real Interest Rates based on the spot rates.
        - NIR: The Nominal Interest Rates interpolated for the maturity date.
        - BEI: The calculated Breakeven Inflation Rates.

    Notes:
        The BEI is calculated by comparing the nominal and real interest rates,
        indicating the market's inflation expectations over the period from the
        settlement date to the bond's maturity.

    Examples:
        Get the NTN-B rates for a specific reference date.
        These are YTM rates and the spot rates are calculated based on them
        >>> df_ntnb = yd.ntnb.data("05-09-2024")

        Get the DI Futures settlement rates for the same reference date to be used as
        reference for the nominal rates:
        >>> df_di = yd.di1.data("05-09-2024")

        Calculate the BEI rates considering the settlement at the reference date:
        >>> yd.ntnb.bei_rates(
        ...     settlement="05-09-2024",
        ...     ntnb_maturities=df_ntnb["MaturityDate"],
        ...     ntnb_rates=df_ntnb["IndicativeRate"],
        ...     nominal_maturities=df_di["ExpirationDate"],
        ...     nominal_rates=df_di["SettlementRate"],
        ... )
        shape: (14, 5)
        ┌──────────────┬─────────┬──────────┬──────────┬──────────┐
        │ MaturityDate ┆ BDToMat ┆ RIR      ┆ NIR      ┆ BEI      │
        │ ---          ┆ ---     ┆ ---      ┆ ---      ┆ ---      │
        │ date         ┆ i64     ┆ f64      ┆ f64      ┆ f64      │
        ╞══════════════╪═════════╪══════════╪══════════╪══════════╡
        │ 2025-05-15   ┆ 171     ┆ 0.061748 ┆ 0.113836 ┆ 0.049059 │
        │ 2026-08-15   ┆ 488     ┆ 0.066133 ┆ 0.117126 ┆ 0.04783  │
        │ 2027-05-15   ┆ 673     ┆ 0.063816 ┆ 0.117169 ┆ 0.050152 │
        │ 2028-08-15   ┆ 988     ┆ 0.063635 ┆ 0.11828  ┆ 0.051376 │
        │ 2029-05-15   ┆ 1172    ┆ 0.062532 ┆ 0.11838  ┆ 0.052561 │
        │ …            ┆ …       ┆ …        ┆ …        ┆ …        │
        │ 2040-08-15   ┆ 3995    ┆ 0.060468 ┆ 0.11759  ┆ 0.053865 │
        │ 2045-05-15   ┆ 5182    ┆ 0.0625   ┆ 0.11759  ┆ 0.05185  │
        │ 2050-08-15   ┆ 6497    ┆ 0.063016 ┆ 0.11759  ┆ 0.051339 │
        │ 2055-05-15   ┆ 7686    ┆ 0.062252 ┆ 0.11759  ┆ 0.052095 │
        │ 2060-08-15   ┆ 9003    ┆ 0.063001 ┆ 0.11759  ┆ 0.051354 │
        └──────────────┴─────────┴──────────┴──────────┴──────────┘
    """
    if has_nullable_args(
        settlement, ntnb_maturities, ntnb_rates, nominal_maturities, nominal_rates
    ):
        return pl.DataFrame()
    # Normalize input dates
    settlement = cv.convert_dates(settlement)
    ntnb_maturities = cv.convert_dates(ntnb_maturities)

    ff_interpolator = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, nominal_maturities),
        known_rates=nominal_rates,
        extrapolate=True,
    )
    df_spot = spot_rates(settlement, ntnb_maturities, ntnb_rates)
    df = (
        df_spot.rename({"SpotRate": "RIR"})
        .with_columns(
            NIR=ff_interpolator(df_spot["BDToMat"]),
        )
        .with_columns(
            BEI=((pl.col("NIR") + 1) / (pl.col("RIR") + 1)) - 1,
        )
        .select("MaturityDate", "BDToMat", "RIR", "NIR", "BEI")
    )

    return df


def duration(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
     Calculate the Macaulay duration of the NTN-B bond in business years.

    Formula:
                   Sum( t * CFₜ / (1 + y)ᵗ )
         MacD = ---------------------------------
                         Current Bond Price

     Where:
         t    = time in years until payment
         CFₜ = cash flow at time t
         y    = yield to maturity (periodic)
         Price = Sum( CFₜ / (1 + y)ᵗ )

     Args:
         settlement (DateLike): The settlement date of the operation.
         maturity (DateLike): The maturity date of the NTN-B bond.
         rate (float): The discount rate used to calculate the duration.

     Returns:
         float: The Macaulay duration of the NTN-B bond in business years.

     Examples:
         >>> from pyield import ntnb
         >>> ntnb.duration("23-08-2024", "15-08-2060", 0.061005)
         15.08305431313046
    """
    if has_nullable_args(settlement, maturity, rate):
        return float("nan")

    df = cash_flows(settlement, maturity)
    if df.is_empty():
        return float("nan")

    byears = bday.count(settlement, df["PaymentDate"]) / 252
    dcf = df["CashFlow"] / (1 + rate) ** byears
    duration = (dcf * byears).sum() / dcf.sum()
    # Truncar para 14 casas decimais para repetibilidade dos resultados
    return tl.truncate(duration, 14)


def dv01(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    vna: float,
) -> float:
    """
    Calculate the DV01 (Dollar Value of 01) for an NTN-B in R$.

    Represents the price change in R$ for a 1 basis point (0.01%) increase in yield.

    Args:
        settlement (DateLike): The settlement date in 'DD-MM-YYYY' format
            or a date-like object.
        maturity (DateLike): The maturity date in 'DD-MM-YYYY' format or
            a date-like object.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-B.

    Returns:
        float: The DV01 value, representing the price change for a 1 basis point
            increase in yield.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.dv01("26-03-2025", "15-08-2060", 0.074358, 4470.979474)
        4.640875999999935
    """
    if has_nullable_args(settlement, maturity, rate, vna):
        return float("nan")

    quotation1 = quotation(settlement, maturity, rate)
    quotation2 = quotation(settlement, maturity, rate + 0.0001)
    price1 = price(vna, quotation1)
    price2 = price(vna, quotation2)
    return price1 - price2


def forwards(
    date: DateLike,
    zero_coupon: bool = True,
) -> pl.DataFrame:
    """
    Calculate the NTN-B forward rates for the given reference date.

    Args:
        date (DateLike): The reference date for fetching the data.
        zero_coupon (bool, optional): If True, use zero-coupon rates for
            forward rate calculation. Defaults to True. If False, the
            yield to maturity rates are used instead.

    Returns:
        pl.DataFrame: A DataFrame containing the forward rates.

    Columns returned:
        - MaturityDate: The maturity date of the bond.
        - BDToMat: The number of business days to maturity.
        - IndicativeRate: The indicative rate for the bond.
        - ForwardRate: The calculated forward rate for the bond.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.forwards("17-10-2025", zero_coupon=True)
        shape: (13, 4)
        ┌──────────────┬─────────┬────────────────┬─────────────┐
        │ MaturityDate ┆ BDToMat ┆ IndicativeRate ┆ ForwardRate │
        │ ---          ┆ ---     ┆ ---            ┆ ---         │
        │ date         ┆ i64     ┆ f64            ┆ f64         │
        ╞══════════════╪═════════╪════════════════╪═════════════╡
        │ 2026-08-15   ┆ 207     ┆ 0.10089        ┆ 0.10089     │
        │ 2027-05-15   ┆ 392     ┆ 0.088776       ┆ 0.074793    │
        │ 2028-08-15   ┆ 707     ┆ 0.083615       ┆ 0.076598    │
        │ 2029-05-15   ┆ 891     ┆ 0.0818         ┆ 0.074148    │
        │ 2030-08-15   ┆ 1205    ┆ 0.080902       ┆ 0.077857    │
        │ …            ┆ …       ┆ …              ┆ …           │
        │ 2040-08-15   ┆ 3714    ┆ 0.076067       ┆ 0.070587    │
        │ 2045-05-15   ┆ 4901    ┆ 0.075195       ┆ 0.069811    │
        │ 2050-08-15   ┆ 6216    ┆ 0.074087       ┆ 0.064348    │
        │ 2055-05-15   ┆ 7405    ┆ 0.073702       ┆ 0.067551    │
        │ 2060-08-15   ┆ 8722    ┆ 0.073795       ┆ 0.074505    │
        └──────────────┴─────────┴────────────────┴─────────────┘
    """
    if has_nullable_args(date):
        return pl.DataFrame()

    # Validate and normalize the date
    df = data(date).select("MaturityDate", "BDToMat", "IndicativeRate")
    if zero_coupon:
        df_ref = spot_rates(
            settlement=date,
            maturities=df["MaturityDate"],
            rates=df["IndicativeRate"],
        ).rename({"SpotRate": "ReferenceRate"})
    else:
        df_ref = df.rename({"IndicativeRate": "ReferenceRate"})
    fwd_rates = fwd.forwards(bdays=df_ref["BDToMat"], rates=df_ref["ReferenceRate"])
    df_ref = df_ref.with_columns(ForwardRate=fwd_rates)
    df = df.join(
        df_ref.select("MaturityDate", "ForwardRate"),
        on="MaturityDate",
        how="inner",
    ).sort("MaturityDate")
    return df
