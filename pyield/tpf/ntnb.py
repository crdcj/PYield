import numpy as np
import pandas as pd

import pyield.date_converter as dc
import pyield.interpolator as ip
import pyield.tpf.tools as bt
from pyield import bday
from pyield.anbima import tpf
from pyield.date_converter import DateScalar

"""
Constants calculated as per Anbima Rules and in base 100
COUPON_RATE = (0.06 + 1) ** 0.5 - 1  # 6% annual rate compounded semi-annually
COUPON_PMT = round(100 * COUPON_RATE, 6) -> 2.956301
FINAL_PMT = principal + last coupon payment = 100 + 2.956301
"""
COUPON_DAY = 15
COUPON_MONTHS = {2, 5, 8, 11}
COUPON_PMT = 2.956301
FINAL_PMT = 102.956301


def _is_maturity_valid(maturity: pd.Timestamp) -> bool:
    """
    Check if the maturity date is a valid NTN-B maturity date.

    Args:
        maturity (pd.Timestamp): The maturity date to be checked.

    Returns:
        bool: True if the maturity date is valid, False otherwise.
    """
    return maturity.day == COUPON_DAY and maturity.month in COUPON_MONTHS


def _check_maturities(
    maturities: pd.Timestamp | list[pd.Timestamp] | pd.Series,
) -> None:
    """
    Check if the maturity dates are valid NTN-B maturities.

    Args:
        maturities (pd.Timestamp | list[pd.Timestamp] | pd.Series): The maturity
            date(s) to be checked.

    Raises:
        ValueError: If the maturity dates are not valid NTN-B maturities.
    """
    if isinstance(maturities, pd.Timestamp):
        maturities = [maturities]
    checked_maturities = [_is_maturity_valid(maturity) for maturity in maturities]
    if not all(checked_maturities):
        raise ValueError("NTN-B maturity must be 15/02, 15/05, 15/08 or 15/11.")


def rates(date: DateScalar) -> pd.DataFrame:
    """
    Fetch the bond indicative rates for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame with columns "MaturityDate" and "IndicativeRate".

    Returned columns:
        - MaturityDate: The maturity date of the bond.
        - IndicativeRate: The indicative rate for the bond.

    Examples:
        >>> yd.ntnb.rates("16-08-2024")
           MaturityDate  IndicativeRate
        0    2025-05-15        0.063893
        1    2026-08-15        0.066095
        2    2027-05-15        0.064164
        3    2028-08-15        0.063199
        4    2029-05-15        0.061753
        5    2030-08-15        0.059997
        6    2032-08-15        0.060025
        7    2033-05-15        0.059876
        8    2035-05-15        0.059557
        9    2040-08-15        0.058873
        10   2045-05-15        0.060013
        11   2050-08-15        0.060247
        12   2055-05-15        0.059926
        13   2060-08-15        0.060179

    """
    ntnb_rates = tpf.tpf_rates(date, "NTN-B")
    if ntnb_rates.empty:
        return pd.DataFrame()
    return ntnb_rates[["MaturityDate", "IndicativeRate"]]


def maturities(date: DateScalar) -> pd.Series:
    """
    Get the bond maturities available for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.Series: Series containing the maturity dates for the NTN-B bonds.

    Examples:

        >>> yd.ntnb.maturities("16-08-2024")
        0    2025-05-15
        1    2026-08-15
        2    2027-05-15
        3    2028-08-15
        4    2029-05-15
        5    2030-08-15
        6    2032-08-15
        7    2033-05-15
        8    2035-05-15
        9    2040-08-15
        10   2045-05-15
        11   2050-08-15
        12   2055-05-15
        13   2060-08-15
        dtype: datetime64[ns]

    """
    df_rates = rates(date)
    s_maturities = df_rates["MaturityDate"]
    s_maturities.name = None
    return s_maturities


def _generate_all_coupon_dates(
    start: DateScalar,
    end: DateScalar,
) -> pd.Series:
    """
    Generate a map of all possible coupon dates between the start and end dates.
    The dates are inclusive. Coupon payments are made on the 15th of February, May,
    August, and November (15-02, 15-05, 15-08, and 15-11 of each year).

    Args:
        start (DateScalar): The start date.
        end (DateScalar): The end date.

    Returns:
        pd.Series: Series of coupon dates within the specified range.
    """
    # Validate and normalize dates
    start = dc.convert_input_dates(start)
    end = dc.convert_input_dates(end)

    # Initialize the first coupon date based on the reference date
    reference_year = start.year
    first_coupon_date = pd.Timestamp(f"{reference_year}-02-01")

    # Generate coupon dates
    coupon_dates = pd.date_range(start=first_coupon_date, end=end, freq="3MS")

    # Offset dates by 14 in order to have day 15 of the month
    coupon_dates += pd.Timedelta(days=14)

    # First coupon date must be after the reference date, otherwise, it can lead to
    # division by zero where BDays == 0 (bootstrap method for instance)
    coupon_dates = coupon_dates[coupon_dates > start]

    return pd.Series(coupon_dates).reset_index(drop=True)


def payment_dates(
    settlement: DateScalar,
    maturity: DateScalar,
) -> pd.Series:
    """
    Generate all remaining coupon dates between a given date and the maturity date.
    The dates are inclusive. Coupon payments are made on the 15th of February, May,
    August, and November (15-02, 15-05, 15-08, and 15-11 of each year). The NTN-B
    bond is determined by its maturity date.

    Args:
        settlement (DateScalar): The settlement date (exlusive) to start generating
            the coupon dates.
        maturity (DateScalar): The maturity date.

    Returns:
        pd.Series: Series of coupon dates within the specified range.

    Examples:
        >>> yd.ntnb.payment_dates("10-05-2024", "15-05-2025")
        0   2024-05-15
        1   2024-11-15
        2   2025-05-15
        dtype: datetime64[ns]
    """
    # Validate and normalize dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)
    _check_maturities(maturity)

    # Check if maturity date is after the start date
    if maturity < settlement:
        raise ValueError("Maturity date must be after the start date.")

    # Initialize loop variables
    coupon_dates = maturity
    cp_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while coupon_dates > settlement:
        cp_dates.append(coupon_dates)
        # Move the coupon date back 6 months
        coupon_dates -= pd.DateOffset(months=6)

    return pd.Series(cp_dates).sort_values(ignore_index=True)


def cash_flows(
    settlement: DateScalar,
    maturity: DateScalar,
) -> pd.DataFrame:
    """
    Generate the cash flows for NTN-B bonds between the settlement and maturity dates.

    Args:
        settlement (DateScalar): The settlement date (exclusive) to start generating
            the cash flows.
        maturity (DateScalar): The maturity date of the bond.

    Returns:
        pd.DataFrame: DataFrame with columns "PaymentDate" and "CashFlow".

    Returned columns:
        - PaymentDate: The payment date of the cash flow
        - CashFlow: Cash flow value for the bond

    Examples:
        >>> yd.ntnb.cash_flows("10-05-2024", "15-05-2025")
          PaymentDate    CashFlow
        0  2024-05-15    2.956301
        1  2024-11-15    2.956301
        2  2025-05-15  102.956301
    """
    # Validate and normalize dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

    # Get the coupon dates between the settlement and maturity dates
    p_dates = payment_dates(settlement, maturity)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    cfs = np.where(p_dates == maturity, FINAL_PMT, COUPON_PMT).tolist()

    # Return a dataframe with the payment dates and cash flows
    return pd.DataFrame(data={"PaymentDate": p_dates, "CashFlow": cfs})


def quotation(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the NTN-B quotation in base 100 using Anbima rules.

    Args:
        settlement (DateScalar): The settlement date of the operation.
        maturity (DateScalar): The maturity date of the NTN-B bond.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-B.

    Returns:
        float: The NTN-B quotation truncated to 4 decimal places.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - The semi-annual coupon is set to 2.956301, which represents a 6% annual
          coupon rate compounded semi-annually and rounded to 6 decimal places as per
          Anbima rules.

    Examples:
        >>> yd.ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)
        99.3651
        >>> yd.ntnb.quotation("31-05-2024", "15-08-2060", 0.061878)
        99.5341
        >>> yd.ntnb.quotation("15-08-2024", "15-08-2032", 0.05929)
        100.6409
    """
    # Validate and normalize dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)
    _check_maturities(maturity)

    cf_df = cash_flows(settlement, maturity)
    cf_dates = cf_df["PaymentDate"]
    cf_values = cf_df["CashFlow"]

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement, cf_dates)

    # Calculate the number of periods truncated as per Anbima rules
    num_of_years = bt.truncate(bdays / 252, 14)

    discount_factor = (1 + rate) ** num_of_years

    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    cf_present_value = (cf_values / discount_factor).round(10)

    # Return the quotation (the dcf sum) truncated as per Anbima rules
    return bt.truncate(cf_present_value.sum(), 4)


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
        >>> yd.ntnb.price(4299.160173, 99.3651)
        4271.864805
        >>> yd.ntnb.price(4315.498383, 100.6409)
        4343.156412
    """
    return bt.truncate(vna * quotation / 100, 6)


def spot_rates(
    settlement: DateScalar,
    maturities: pd.Series,
    rates: pd.Series,
    show_coupons: bool = False,
) -> pd.DataFrame:
    """
    Calculate the spot rates for NTN-B bonds using the bootstrap method.

    The bootstrap method is a process used to determine spot rates from
    the yields of a series of bonds. It involves iteratively solving for
    the spot rates that discount each bond's cash flows to its current
    price.

    Args:
        settlement (DateScalar): The reference date for settlement.
        maturities (pd.Series): Series of maturity dates for the bonds.
        rates (pd.Series): Series of yield to maturity rates.
        show_coupons (bool, optional): If True, the result will include the
            intermediate coupon dates. Defaults to False.

    Returns:
        pd.DataFrame: DataFrame with columns "MaturityDate", "SpotRate".

    Examples:
        # Get the NTN-B rates for a specific reference date
        >>> df_rates = yd.ntnb.rates("16-08-2024")

        # Calculate the spot rates considering the settlement at the reference date
        >>> yd.ntnb.spot_rates(
        ...     settlement="16-08-2024",
        ...     maturities=df_rates["MaturityDate"],
        ...     rates=df_rates["IndicativeRate"],
        ... )
           MaturityDate  BDToMat  SpotRate
        0    2025-05-15      185  0.063894
        1    2026-08-15      502  0.066141
        2    2027-05-15      687  0.064087
        3    2028-08-15     1002  0.063057
        4    2029-05-15     1186  0.061458
        5    2030-08-15     1500  0.059491
        6    2032-08-15     2004  0.059652
        7    2033-05-15     2191  0.059497
        8    2035-05-15     2690  0.059151
        9    2040-08-15     4009  0.058326
        10   2045-05-15     5196  0.060371
        11   2050-08-15     6511  0.060772
        12   2055-05-15     7700  0.059909
        13   2060-08-15     9017  0.060652

    Notes:
        The calculation of the spot rates for NTN-B bonds considers the following steps:
            - Map all all possible payment dates up to the longest maturity date.
            - Interpolate the YTM rates in the intermediate payment dates.
            - Calculate the NTN-B quotation for each maturity date.
            - Calculate the real spot rates for each maturity date.
            - Columns in the returned DataFrame:
                - MaturityDate: The maturity date of the bond.
                - BDToMat: The number of business days from settlement to maturities.
                - SpotRate: The real spot rate for the bond.
    """
    # Process and validate the input data
    settlement = dc.convert_input_dates(settlement)
    maturities = pd.to_datetime(maturities, errors="raise", dayfirst=True)
    _check_maturities(maturities)

    # Create the interpolator to calculate the YTM rates for intermediate dates
    ff_interpolator = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, maturities),
        known_rates=rates,
    )

    # Generate coupon dates up to the longest maturity date
    all_coupon_dates = _generate_all_coupon_dates(
        start=settlement, end=maturities.max()
    )

    # Create a DataFrame with all coupon dates and the corresponding YTM
    df = pd.DataFrame(data=all_coupon_dates, columns=["MaturityDate"])
    df["BDToMat"] = bday.count(settlement, df["MaturityDate"])
    df["BYears"] = df["BDToMat"] / 252
    df["YTM"] = df["BDToMat"].apply(ff_interpolator)
    df["Coupon"] = COUPON_PMT
    df["SpotRate"] = np.nan

    # The Bootstrap loop to calculate spot rates
    for index, row in df.iterrows():
        # Get the cash flow dates for the bond
        cf_dates = payment_dates(settlement, row["MaturityDate"])

        # If there is only one cash flow date, it means the bond is a single payment
        # bond, so the spot rate is equal to the YTM rate
        if len(cf_dates) == 1:
            df.at[index, "SpotRate"] = row["YTM"]
            continue

        # Calculate the present value of the cash flows without last payment
        cf_dates = cf_dates[:-1]
        cf_df = df.query("MaturityDate in @cf_dates").reset_index(drop=True)
        cf_present_value = bt.calculate_present_value(
            cash_flows=cf_df["Coupon"],
            rates=cf_df["SpotRate"],
            periods=cf_df["BYears"],
        )

        # Calculate the Spot Rate for the bond
        bond_price = quotation(settlement, row["MaturityDate"], row["YTM"])
        price_factor = FINAL_PMT / (bond_price - cf_present_value)
        df.at[index, "SpotRate"] = price_factor ** (1 / row["BYears"]) - 1

    df = df[["MaturityDate", "BDToMat", "SpotRate"]].copy()
    # Force Float64 type in float columns to standardize the output
    df["SpotRate"] = df["SpotRate"].astype("Float64")

    # Filter the result without the intermediate coupon dates (virtual bonds)
    if not show_coupons:
        df = df.query("MaturityDate in @maturities").reset_index(drop=True)
    return df


def bei_rates(
    settlement: DateScalar,
    ntnb_maturities: pd.Series,
    ntnb_rates: pd.Series,
    nominal_maturities: pd.Series,
    nominal_rates: pd.Series,
) -> pd.DataFrame:
    """
    Calculate the Breakeven Inflation (BEI) for NTN-B bonds based on nominal and real
    interest rates. The BEI represents the inflation rate that equalizes the real and
    nominal yields. The calculation is based on the spot rates for NTN-B bonds.

    Args:
        settlement (str or pd.Timestamp): The settlement date of the operation.
        ntnb_maturities (pd.Series): The maturity dates for the NTN-B bonds.
        ntnb_rates (pd.Series): The real interest rates (Yield to Maturity - YTM)
            corresponding to the given NTN-B maturities.
        nominal_maturities (pd.Series): The maturity dates to be used as reference for
            nominal reates.
        nominal_rates (pd.Series): The nominal interest rates (e.g. DI Futures or
             zero prefixed bonds rates) used as reference for the calculation.

    Returns:
        pd.DataFrame: DataFrame containing the calculated breakeven inflation rates.

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
        >>> df_ntnb = yd.ntnb.rates("05-09-2024")

        Get the DI Futures settlement rates for the same reference date to be used as
        reference for the nominal rates:
        >>> di = yd.DIFutures("05-09-2024")
        >>> df_di = di.df

        Calculate the BEI rates considering the settlement at the reference date:
        >>> yd.ntnb.bei_rates(
        ...     settlement="05-09-2024",
        ...     ntnb_maturities=df_ntnb["MaturityDate"],
        ...     ntnb_rates=df_ntnb["IndicativeRate"],
        ...     nominal_maturities=df_di["ExpirationDate"],
        ...     nominal_rates=df_di["SettlementRate"],
        ... )
           MaturityDate  BDToMat       RIR       NIR       BEI
        0    2025-05-15      171  0.061749  0.113836  0.049058
        1    2026-08-15      488  0.066133  0.117126   0.04783
        2    2027-05-15      673  0.063816  0.117169  0.050152
        3    2028-08-15      988  0.063635   0.11828  0.051376
        4    2029-05-15     1172  0.062532   0.11838  0.052561
        5    2030-08-15     1486  0.061809  0.118499   0.05339
        6    2032-08-15     1990  0.062135  0.118084  0.052676
        7    2033-05-15     2177  0.061897   0.11787   0.05271
        8    2035-05-15     2676  0.061711  0.117713  0.052747
        9    2040-08-15     3995  0.060468   0.11759  0.053865
        10   2045-05-15     5182    0.0625   0.11759   0.05185
        11   2050-08-15     6497  0.063016   0.11759  0.051339
        12   2055-05-15     7686  0.062252   0.11759  0.052095
        13   2060-08-15     9003  0.063001   0.11759  0.051354

    """
    # Normalize input dates
    settlement = dc.convert_input_dates(settlement)
    ntnb_maturities = pd.to_datetime(ntnb_maturities, errors="coerce", dayfirst=True)

    # Calculate Real Interest Rate (RIR)
    df = spot_rates(settlement, ntnb_maturities, ntnb_rates)
    df["BDToMat"] = bday.count(settlement, df["MaturityDate"])
    df = df.rename(columns={"SpotRate": "RIR"})

    nir_interplator = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, nominal_maturities),
        known_rates=nominal_rates,
        extrapolate=True,
    )

    df["NIR"] = df["BDToMat"].apply(nir_interplator).astype("Float64")
    # Calculate Breakeven Inflation Rate (BEI)
    df["BEI"] = ((df["NIR"] + 1) / (df["RIR"] + 1)) - 1

    cols_reordered = [
        "MaturityDate",
        "BDToMat",
        "RIR",
        "NIR",
        "BEI",
    ]
    return df[cols_reordered].copy()


def duration(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

    df = cash_flows(settlement, maturity)
    df["BY"] = bday.count(settlement, df["PaymentDate"]) / 252
    df["DCF"] = df["CashFlow"] / (1 + rate) ** df["BY"]
    np_duration = (df["DCF"] * df["BY"]).sum() / df["DCF"].sum()
    return float(np_duration)
