import pandas as pd

from pyield import bday
from pyield import date_converter as dc
from pyield.anbima import tpf
from pyield.b3 import di
from pyield.date_converter import DateScalar
from pyield.tpf import tools as tt

FACE_VALUE = 1000


def rates(date: DateScalar) -> pd.DataFrame:
    """
    Fetch the LTN Anbima indicative rates for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame with columns "MaturityDate" and "IndicativeRate".

    Examples:
        >>> yd.ltn.rates("22-08-2024")
           MaturityDate  IndicativeRate
        0    2024-10-01        0.104444
        1    2025-01-01        0.107555
        2    2025-04-01        0.111592
        3    2025-07-01         0.11387
        4    2025-10-01        0.115483
        5    2026-01-01        0.116013
        6    2026-04-01        0.116294
        7    2026-07-01        0.116743
        8    2026-10-01        0.116767
        9    2027-07-01        0.116982
        10   2028-01-01        0.117289
        11   2028-07-01        0.117948
        12   2030-01-01        0.118746

    """
    ltn_rates = tpf.tpf_rates(date, "LTN")
    if ltn_rates.empty:
        return pd.DataFrame()
    return ltn_rates[["MaturityDate", "IndicativeRate"]]


def maturities(date: DateScalar) -> pd.Series:
    """
    Fetch the bond maturities available for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.Series: A Series of bond maturities available for the reference date.

    Examples:
        >>> yd.ltn.maturities("22-08-2024")
        0    2024-10-01
        1    2025-01-01
        2    2025-04-01
        3    2025-07-01
        4    2025-10-01
        5    2026-01-01
        6    2026-04-01
        7    2026-07-01
        8    2026-10-01
        9    2027-07-01
        10   2028-01-01
        11   2028-07-01
        12   2030-01-01
        dtype: datetime64[ns]

    """
    df_rates = rates(date)
    s_maturities = df_rates["MaturityDate"]
    s_maturities.name = None
    return s_maturities


def price(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the LTN price using Anbima rules.

    Args:
        settlement (DateScalar): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (DateScalar): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-F.

    Returns:
        float: The LTN price using Anbima rules.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> yd.ltn.price("05-07-2024", "01-01-2030", 0.12145)
        535.279902
    """

    # Validate and normalize dates
    settlement = dc.convert_input_dates(settlement)
    maturity = dc.convert_input_dates(maturity)

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement, maturity)

    # Calculate the number of periods truncated as per Anbima rule
    num_of_years = tt.truncate(bdays / 252, 14)

    discount_factor = (1 + rate) ** num_of_years

    # Truncate the price to 6 decimal places as per Anbima rules
    return tt.truncate(FACE_VALUE / discount_factor, 6)


def di_spreads(date: DateScalar) -> pd.DataFrame:
    """
    Calculates the DI spread for the LTN based on ANBIMA's indicative rates.

    This function fetches the indicative rates for the NTN-F bonds and the DI futures
    rates and calculates the spread between these rates in basis points.

    Parameters:
        date (DateScalar, optional): The reference date for the
            spread calculation.

    Returns:
        pd.DataFrame: DataFrame with the columns "MaturityDate" and "DISpread" (in bps).

    Examples:
        >>> yd.ltn.di_spreads("22-08-2024")
           MaturityDate  DISpread
        0    2024-10-01     -3.06
        1    2025-01-01     -9.95
        2    2025-04-01     -8.28
        3    2025-07-01      -6.1
        4    2025-10-01     -2.57
        5    2026-01-01     -1.57
        6    2026-04-01     -0.86
        7    2026-07-01      2.83
        8    2026-10-01      4.17
        9    2027-07-01      3.72
        10   2028-01-01      6.19
        11   2028-07-01      8.68
        12   2030-01-01     14.96
    """
    # Fetch DI Spreads for the reference date
    df = tt.pre_spreads(date)
    df.query("BondType == 'LTN'", inplace=True)
    df.sort_values(["MaturityDate"], ignore_index=True, inplace=True)
    return df[["MaturityDate", "DISpread"]]


def premium(ltn_rate: float, di_rate: float) -> float:
    """
    Calculate the premium of the LTN bond over the DI Future rate using provided rates.

    Args:
        ltn_rate (float): The annualized LTN rate.
        di_rate (float): The annualized DI Future rate.

    Returns:
        float: The premium of the LTN bond over the DI Future rate.

    Examples:
        Reference date: 22-08-2024
        LTN rate for 01-01-2030: 0.118746
        DI (JAN30) Settlement rate: 0.11725
        >>> yd.ltn.premium(0.118746, 0.11725)
        1.0120718007994287
    """
    # Cálculo das taxas diárias
    ltn_factor = (1 + ltn_rate) ** (1 / 252)
    di_factor = (1 + di_rate) ** (1 / 252)

    # Retorno do cálculo do prêmio
    return (ltn_factor - 1) / (di_factor - 1)


def historical_premium(
    date: DateScalar,
    maturity: DateScalar,
) -> float:
    """
    Calculate the premium of the LTN bond over the DI Future rate for a given date.

    Args:
        date (DateScalar): The reference date to fetch the rates.
        maturity (DateScalar): The maturity date of the LTN bond.

    Returns:
        float: The premium of the LTN bond over the DI Future rate for the given date.
               If the data is not available, returns NaN.

    Examples:
        >>> yd.ltn.historical_premium("22-08-2024", "01-01-2030")
        1.0120718007994287

    """
    # Convert input dates to a consistent format
    date = dc.convert_input_dates(date)
    maturity = dc.convert_input_dates(maturity)

    # Retrieve LTN rates for the reference date
    df_anbima = rates(date)
    if df_anbima.empty:
        return float("NaN")

    # Extract the LTN rate for the specified maturity date
    ltn_rates = df_anbima.query("MaturityDate == @maturity")["IndicativeRate"]
    if ltn_rates.empty:
        return float("NaN")
    ltn_rate = float(ltn_rates.iloc[0])

    # Retrieve DI rate for the reference date and maturity
    dif = di.DIFutures(date=date)
    di_rate = dif.rate(expiration=maturity, interpolate=True, extrapolate=False)
    if pd.isnull(di_rate):  # Check if the DI rate is NaN
        return float("NaN")

    # Calculate and return the premium using the extracted rates
    return premium(ltn_rate, di_rate)
