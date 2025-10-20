import math
from collections.abc import Callable

import pandas as pd
import polars as pl

import pyield.converters as cv
import pyield.interpolator as ip
from pyield import anbima, bday
from pyield.tn import tools
from pyield.tn.pre import di_spreads as pre_di_spreads
from pyield.types import DateArray, DateScalar, FloatArray, has_null_args

"""
Constants calculated as per Anbima Rules
COUPON_RATE = (0.10 + 1) ** 0.5 - 1  -> 10% annual rate compounded semi-annually
FACE_VALUE = 1000
COUPON_PMT = round(FACE_VALUE * COUPON_RATE, 5)
FINAL_PMT = FACE_VALUE + COUPON_PMT
"""
COUPON_DAY = 1
COUPON_MONTHS = {1, 7}
COUPON_PMT = 48.80885
FINAL_PMT = 1048.80885  # 1000 + 48.80885


def data(date: DateScalar) -> pl.DataFrame:
    """
    Fetch the bond indicative rates for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pl.DataFrame: DataFrame with columns "MaturityDate" and "IndicativeRate".

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.data("23-08-2024")
        shape: (6, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬──────────┬──────────┬────────────────┬─────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate  ┆ AskRate  ┆ IndicativeRate ┆ DIRate  │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---      ┆ ---      ┆ ---            ┆ ---     │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64      ┆ f64      ┆ f64            ┆ f64     │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪══════════╪══════════╪════════════════╪═════════╡
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2014-01-10    ┆ … ┆ 0.107864 ┆ 0.107524 ┆ 0.107692       ┆ 0.10823 │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2016-01-15    ┆ … ┆ 0.11527  ┆ 0.114948 ┆ 0.115109       ┆ 0.11467 │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2018-01-05    ┆ … ┆ 0.116468 ┆ 0.11621  ┆ 0.116337       ┆ 0.1156  │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2020-01-10    ┆ … ┆ 0.117072 ┆ 0.116958 ┆ 0.117008       ┆ 0.11575 │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2022-01-07    ┆ … ┆ 0.116473 ┆ 0.116164 ┆ 0.116307       ┆ 0.11554 │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2024-01-05    ┆ … ┆ 0.116662 ┆ 0.116523 ┆ 0.116586       ┆ 0.11531 │
        └───────────────┴──────────┴───────────┴───────────────┴───┴──────────┴──────────┴────────────────┴─────────┘
    """  # noqa
    return anbima.tpf_data(date, "NTN-F")


def maturities(date: DateScalar) -> pl.Series:
    """
    Fetch the NTN-F bond maturities available for the given reference date.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pl.Series: A Series of NTN-F bond maturities available for the reference date.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.maturities("23-08-2024")
        shape: (6,)
        Series: 'MaturityDate' [date]
        [
            2025-01-01
            2027-01-01
            2029-01-01
            2031-01-01
            2033-01-01
            2035-01-01
        ]
    """
    return data(date)["MaturityDate"]


def payment_dates(
    settlement: DateScalar,
    maturity: DateScalar,
) -> pl.Series:
    """
    Generate all remaining coupon dates between a settlement date and a maturity date.
    The dates are exclusive for the settlement date and inclusive for the maturity date.
    Coupon payments are made on the 1st of January and July.
    The NTN-F bond is determined by its maturity date.

    Args:
        settlement (DateScalar): The settlement date.
        maturity (DateScalar): The maturity date.

    Returns:
        pl.Series: A Series containing the coupon dates between the settlement
            (exclusive) and maturity (inclusive) dates.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.payment_dates("15-05-2024", "01-01-2027")
        shape: (6,)
        Series: '' [date]
        [
            2024-07-01
            2025-01-01
            2025-07-01
            2026-01-01
            2026-07-01
            2027-01-01
        ]
    """
    if has_null_args(settlement, maturity):
        return pl.Series(dtype=pl.Date)
    # Normalize dates
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    # Check if maturity date is after the start date
    if maturity <= settlement:
        raise ValueError("Maturity date must be after the settlement date.")

    # Initialize loop variables
    coupon_date = maturity
    coupon_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while coupon_date > settlement:
        coupon_dates.append(coupon_date)
        # Move the coupon date back 6 months
        coupon_date -= pd.DateOffset(months=6)
        coupon_date = coupon_date.date()  # DateOffset returns a Timestamp

    return pl.Series(coupon_dates).sort()


def cash_flows(
    settlement: DateScalar,
    maturity: DateScalar,
    adj_payment_dates: bool = False,
) -> pl.DataFrame:
    """
    Generate the cash flows for the NTN-F bond between the settlement (exclusive) and
    maturity dates (inclusive). The cash flows are the coupon payments and the final
    payment at maturity.

    Args:
        settlement (DateScalar): The date (exclusive) for starting the cash flows.
        maturity (DateScalar): The maturity date of the bond.
        adj_payment_dates (bool): If True, adjust the payment dates to the next
            business day.

    Returns:
        pl.DataFrame: DataFrame with columns "PaymentDate" and "CashFlow".

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.cash_flows("15-05-2024", "01-01-2027")
        shape: (6, 2)
        ┌─────────────┬────────────┐
        │ PaymentDate ┆ CashFlow   │
        │ ---         ┆ ---        │
        │ date        ┆ f64        │
        ╞═════════════╪════════════╡
        │ 2024-07-01  ┆ 48.80885   │
        │ 2025-01-01  ┆ 48.80885   │
        │ 2025-07-01  ┆ 48.80885   │
        │ 2026-01-01  ┆ 48.80885   │
        │ 2026-07-01  ┆ 48.80885   │
        │ 2027-01-01  ┆ 1048.80885 │
        └─────────────┴────────────┘
    """
    if has_null_args(settlement, maturity):
        return pl.DataFrame()
    # Normalize input dates
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    # Get the payment dates between the settlement and maturity dates
    pay_dates = payment_dates(settlement, maturity)

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    df = pl.DataFrame(data={"PaymentDate": pay_dates}).with_columns(
        pl.when(pl.col("PaymentDate") == maturity)
        .then(FINAL_PMT)
        .otherwise(COUPON_PMT)
        .alias("CashFlow")
    )

    if adj_payment_dates:
        adj_pay_dates = bday.offset(pay_dates, 0)
        df = df.with_columns(PaymentDate=adj_pay_dates)
    return df


def price(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float | None:
    """
    Calculate the NTN-F price using Anbima rules, which corresponds to the present
        value of the cash flows discounted at the given yield to maturity rate (YTM).

    Args:
        settlement (DateScalar): The settlement date to calculate the price.
        maturity (DateScalar): The maturity date of the bond.
        rate (float): The discount rate (yield to maturity) used to calculate the
            present value of the cash flows.

    Returns:
        float | None: The NTN-F price using Anbima rules.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - The semi-annual coupon is set to 48.81, which represents a 10% annual
          coupon rate compounded semi-annually and rounded to 5 decimal places as per
          Anbima rules.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.price("05-07-2024", "01-01-2035", 0.11921)
        895.359254
    """
    if has_null_args(settlement, maturity, rate):
        return None
    cf_df = cash_flows(settlement, maturity)
    cf_values = cf_df["CashFlow"]
    bdays = bday.count(settlement, cf_df["PaymentDate"])
    byears = tools.truncate(bdays / 252, 14)
    discount_factors = (1 + rate) ** byears
    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    dcf = (cf_values / discount_factors).round(9)
    # Return the sum of the discounted cash flows truncated as per Anbima rules
    return tools.truncate(dcf.sum(), 6)


def spot_rates(  # noqa
    settlement: DateScalar,
    ltn_maturities: DateArray,
    ltn_rates: FloatArray,
    ntnf_maturities: DateArray,
    ntnf_rates: FloatArray,
    show_coupons: bool = False,
) -> pl.DataFrame:
    """
    Calculate the spot rates (zero coupon rates) for NTN-F bonds using the bootstrap
    method.

    The bootstrap method is a process used to determine spot rates from
    the yields of a series of bonds. It involves iteratively solving for
    the spot rates that discount each bond's cash flows to its current
    price. It uses the LTN rates, which are zero coupon bonds, up to the
    last LTN maturity available. For maturities after the last LTN maturity,
    it calculates the spot rates using the bootstrap method.


    Args:
        settlement (DateScalar): The settlement date for the spot rates calculation.
        ltn_maturities (DateArray): The LTN known maturities.
        ltn_rates (FloatArray): The LTN known rates.
        ntnf_maturities (DateArray): The NTN-F known maturities.
        ntnf_rates (FloatArray): The NTN-F known rates.
        show_coupons (bool): If True, show also July rates corresponding to the
            coupon payments. Defaults to False.

    Returns:
        pl.DataFrame: DataFrame with columns "MaturityDate", "BDToMat" and
            "SpotRate". "BDToMat" is the business days from the settlement date
            to the maturities.

    Examples:
        >>> from pyield import ntnf, ltn
        >>> df_ltn = ltn.data("03-09-2024")
        >>> df_ntnf = ntnf.data("03-09-2024")
        >>> ntnf.spot_rates(
        ...     settlement="03-09-2024",
        ...     ltn_maturities=df_ltn["MaturityDate"],
        ...     ltn_rates=df_ltn["IndicativeRate"],
        ...     ntnf_maturities=df_ntnf["MaturityDate"],
        ...     ntnf_rates=df_ntnf["IndicativeRate"],
        ... )
        shape: (6, 3)
        ┌──────────────┬─────────┬──────────┐
        │ MaturityDate ┆ BDToMat ┆ SpotRate │
        │ ---          ┆ ---     ┆ ---      │
        │ date         ┆ i64     ┆ f64      │
        ╞══════════════╪═════════╪══════════╡
        │ 2025-01-01   ┆ 83      ┆ 0.108837 │
        │ 2027-01-01   ┆ 584     ┆ 0.119981 │
        │ 2029-01-01   ┆ 1083    ┆ 0.122113 │
        │ 2031-01-01   ┆ 1584    ┆ 0.122231 │
        │ 2033-01-01   ┆ 2088    ┆ 0.121355 │
        │ 2035-01-01   ┆ 2587    ┆ 0.121398 │
        └──────────────┴─────────┴──────────┘
    """
    if has_null_args(
        settlement, ltn_maturities, ltn_rates, ntnf_maturities, ntnf_rates
    ):
        return pl.DataFrame()
    # 1. Converter e normalizar inputs para Polars
    settlement = cv.convert_dates(settlement)
    ltn_maturities = cv.convert_dates(ltn_maturities)
    ntnf_maturities = cv.convert_dates(ntnf_maturities)
    if not isinstance(ltn_rates, pl.Series):
        ltn_rates = pl.Series(ltn_rates).cast(pl.Float64)
    if not isinstance(ntnf_rates, pl.Series):
        ntnf_rates = pl.Series(ntnf_rates).cast(pl.Float64)

    # 2. Criar interpoladores (aceitam pl.Series diretamente)
    ltn_rate_interpolator = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, ltn_maturities),
        known_rates=ltn_rates,
    )
    ntnf_rate_interpolator = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, ntnf_maturities),
        known_rates=ntnf_rates,
    )

    # 3. Gerar todas as datas de cupom até o último vencimento NTN-F
    last_maturity = ntnf_maturities.max()
    all_coupon_dates = payment_dates(settlement, last_maturity)

    # 4. Construir DataFrame inicial
    bdays_to_mat = bday.count(settlement, all_coupon_dates)
    df = pl.DataFrame(
        {
            "MaturityDate": all_coupon_dates,
            "BDToMat": bdays_to_mat,
        }
    ).with_columns(
        BYears=pl.col("BDToMat") / 252,
        Coupon=COUPON_PMT,
        YTM=pl.col("BDToMat").map_elements(
            ntnf_rate_interpolator, return_dtype=pl.Float64
        ),
    )

    # 5. Loop de bootstrap (iterativo por dependência sequencial)
    last_ltn_maturity = ltn_maturities.max()
    maturities_list = df["MaturityDate"]
    bdays_list = df["BDToMat"]
    byears_list = df["BYears"]
    ytm_list = df["YTM"]

    solved_spot_rates: list[float] = []
    spot_map: dict[pl.Date, float] = {}

    for i in range(len(df)):
        mat_date = maturities_list[i]
        bdays_val = int(bdays_list[i])
        byears_val = float(byears_list[i])
        ytm_val = float(ytm_list[i])

        # Caso esteja antes (ou igual) ao último vencimento LTN: usar interpolador LTN
        if mat_date <= last_ltn_maturity:
            spot_rate = ltn_rate_interpolator(bdays_val)
            solved_spot_rates.append(spot_rate)
            spot_map[mat_date] = spot_rate
            continue

        # Datas de cupom (exclui último pagamento) para este vencimento
        cf_dates = payment_dates(settlement, mat_date)[:-1]
        if len(cf_dates) == 0:
            # Caso improvável, mas protege contra divisão por zero mais adiante
            spot_rate = None
            solved_spot_rates.append(spot_rate)
            spot_map[mat_date] = spot_rate
            continue

        # Recuperar SpotRates já solucionadas para estes cupons
        cf_spot_rates = [spot_map[d] for d in cf_dates]
        cf_periods = bday.count(settlement, cf_dates) / 252
        cf_cash_flows = [COUPON_PMT] * len(cf_dates)

        cf_present_value = tools.calculate_present_value(
            cash_flows=pl.Series(cf_cash_flows),
            rates=pl.Series(cf_spot_rates),
            periods=cf_periods,
        )

        bond_price = price(settlement, mat_date, ytm_val)
        price_factor = FINAL_PMT / (bond_price - cf_present_value)
        spot_rate = price_factor ** (1 / byears_val) - 1

        solved_spot_rates.append(spot_rate)
        spot_map[mat_date] = spot_rate

    # 6. Anexar coluna SpotRate
    df = df.with_columns(SpotRate=pl.Series(solved_spot_rates, dtype=pl.Float64))

    # 7. Selecionar colunas finais
    df = df.select(["MaturityDate", "BDToMat", "SpotRate"])

    # 8. Remover cupons (Julho) se não solicitado
    if not show_coupons:
        df = df.filter(pl.col("MaturityDate").is_in(ntnf_maturities.implode()))

    return df


def _bisection_method(
    func: Callable[[float], float],
    a: float,
    b: float,
    tol: float = 1e-8,
    maxiter: int = 100,
) -> float:
    """Bisection method for root finding.

    Args:
        func (Callable[[float], float]): Function for which the root is sought. Must
            accept a single float and return a float.
        a (float): Lower bound of the interval.
        b (float): Upper bound of the interval.
        tol (float): Tolerance for convergence. Defaults to 1e-8.
        maxiter (int): Maximum number of iterations allowed. Defaults to 100.

    Returns:
        float: Approximate root of ``func`` within the interval ``[a, b]``.

    Raises:
        ValueError: If ``func`` does not change sign in the interval ``[a, b]``.
    """
    fa, fb = func(a), func(b)
    if fa * fb > 0:
        raise ValueError("Function does not change sign in the interval.")

    for _ in range(maxiter):
        midpoint = (a + b) / 2
        fmid = func(midpoint)
        if abs(fmid) < tol or (b - a) / 2 < tol:
            return midpoint
        if fmid * fa < 0:
            b, fb = midpoint, fmid
        else:
            a, fa = midpoint, fmid

    return (a + b) / 2


def _solve_spread(
    price_difference_func: Callable,
    initial_guess: float | None = None,
) -> float:
    """
    Solve for the spread that zeroes the price difference using a bisection method.

    Args:
        price_difference_func (callable): The function that computes the difference
            between the bond's market price and its discounted cash flows.
        initial_guess (float, optional): An initial guess for the spread.

    Returns:
        float: The solution for the spread in bps or NaN if no solution is found.
    """
    try:
        if initial_guess is not None:
            # range_width_bps below the initial guess
            a = initial_guess - 0.005  # 50 bps
            # range_width_bps above the initial guess
            b = initial_guess + 0.005  # 50 bps
        else:
            a = -0.01  # Initial guess of -100 bps
            b = 0.01  # Initial guess of 100 bps

        # Find the spread (p) that zeroes the price difference
        p_solution = _bisection_method(price_difference_func, a, b)
    except ValueError:
        # If no solution is found, return NaN
        p_solution = float("nan")

    return p_solution


def di_net_spread(  # noqa
    settlement: DateScalar,
    ntnf_maturity: DateScalar,
    ntnf_rate: float,
    di_expirations: DateScalar,
    di_rates: FloatArray,
    initial_guess: float | None = None,
) -> float | None:
    """
    Calculate the net DI spread for a bond given the YTM and the DI rates.

    This function determines the spread over the DI curve that equates the present value
    of the bond's cash flows to its market price. It interpolates the DI rates to match
    the bond's cash flow payment dates and uses the Brent method to find the spread
    (in bps) that zeroes the difference between the bond's market price and its
    discounted cash flows.

    Args:
        settlement (DateScalar): The settlement date to calculate the spread.
        ntnf_maturity (DateScalar): The bond maturity date.
        ntnf_rate (float): The yield to maturity (YTM) of the bond.
        di_rates (FloatArray): A Series of DI rates.
        di_expirations (DateArray): A list or Series of DI expiration dates.
        initial_guess (float, optional): An initial guess for the spread. Defaults to
            None. A good initial guess is the DI gross spread for the bond.

    Returns:
        float | None: The net DI spread in decimal format (e.g., 0.0012 for 12 bps).

    Examples:
        # Obs: only some of the DI rates will be used in the example.
        >>> exp_dates = ["2025-01-01", "2030-01-01", "2035-01-01"]
        >>> di_rates = [0.10823, 0.11594, 0.11531]
        >>> spread = di_net_spread(
        ...     settlement="23-08-2024",
        ...     ntnf_maturity="01-01-2035",
        ...     ntnf_rate=0.116586,
        ...     di_expirations=exp_dates,
        ...     di_rates=di_rates,
        ... )
        >>> round(spread * 10_000, 2)  # Convert to bps for display
        12.13
    """
    # 1. Validação e conversão de inputs
    if has_null_args(settlement, ntnf_maturity, ntnf_rate, di_expirations, di_rates):
        return None
    settlement = cv.convert_dates(settlement)
    ntnf_maturity = cv.convert_dates(ntnf_maturity)
    di_expirations = cv.convert_dates(di_expirations)

    # Force di_rates to be a Polars Series
    if not isinstance(di_rates, pl.Series):
        di_rates = pl.Series(di_rates)

    # 2. Validação dos inputs de DI
    if len(di_rates) != len(di_expirations):
        raise ValueError("di_rates and di_expirations must have the same length.")

    # 3. Criação do interpolador
    ff_interpolator = ip.Interpolator(
        "flat_forward",
        bday.count(settlement, di_expirations),
        di_rates,
    )

    # 4. Geração dos fluxos de caixa do NTN-F
    df = cash_flows(settlement, ntnf_maturity)

    bdays_to_payment = bday.count(settlement, df["PaymentDate"])
    byears_to_payment = bdays_to_payment / 252

    df = df.with_columns(
        BDaysToPayment=bdays_to_payment,
    ).with_columns(
        DIRateInterp=pl.col("BDaysToPayment").map_elements(
            ff_interpolator, return_dtype=pl.Float64
        ),
    )

    # 5. Extração dos dados para o cálculo numérico
    bond_price = price(settlement, ntnf_maturity, ntnf_rate)
    bond_cash_flows = df["CashFlow"]
    di_interp = df["DIRateInterp"]

    # 6. Função de diferença de preço para o solver
    def price_difference(p: float) -> float:
        discounted_cf = bond_cash_flows / (1 + di_interp + p) ** byears_to_payment
        return discounted_cf.sum() - bond_price

    # 7. Resolver para o spread
    return _solve_spread(price_difference, initial_guess)


def premium(
    settlement: DateScalar,
    ntnf_maturity: DateScalar,
    ntnf_rate: float,
    di_expirations: DateScalar,
    di_rates: FloatArray,
) -> float | None:
    """
    Calculate the premium of an NTN-F bond over DI rates.

    This function computes the premium of an NTN-F bond by comparing its implied
    discount factor with that of the DI curve. It determines the net premium based
    on the difference between the discount factors of the bond's yield-to-maturity
    (YTM) and the interpolated DI rates.

    Args:
        settlement (DateScalar): The settlement date to calculate the premium.
        ntnf_maturity (DateScalar): The maturity date of the NTN-F bond.
        ntnf_rate (float): The yield to maturity (YTM) of the NTN-F bond.
        di_expirations (DateScalar): Series with the expiration dates for the DI.
        di_rates (FloatArray): Series containing the DI rates corresponding to
            the expiration dates.

    Returns:
        float | None: The premium of the NTN-F bond over the DI curve, expressed as a
        factor.

    Examples:
        >>> # Obs: only some of the DI rates will be used in the example.
        >>> exp_dates = ["2025-01-01", "2030-01-01", "2035-01-01"]
        >>> di_rates = [0.10823, 0.11594, 0.11531]
        >>> premium(
        ...     settlement="23-08-2024",
        ...     ntnf_maturity="01-01-2035",
        ...     ntnf_rate=0.116586,
        ...     di_expirations=exp_dates,
        ...     di_rates=di_rates,
        ... )
        1.0099602136954626

    Notes:
        - The function adjusts coupon payment dates to business days and calculates
          the present value of cash flows for the NTN-F bond using DI rates.

    """
    if has_null_args(settlement, ntnf_maturity, ntnf_rate, di_expirations, di_rates):
        return None
    # 1. Validação e conversão de datas (padrão consistente)
    settlement = cv.convert_dates(settlement)
    ntnf_maturity = cv.convert_dates(ntnf_maturity)
    di_expirations = cv.convert_dates(di_expirations)
    if not isinstance(di_rates, pl.Series):
        di_rates = pl.Series(di_rates)

    # 2. Preparação do DataFrame de fluxo de caixa e interpolador
    df_cf = cash_flows(settlement, ntnf_maturity, adj_payment_dates=True)

    ff_interpolator = ip.Interpolator(
        "flat_forward",
        bday.count(settlement, di_expirations),
        di_rates,
    )

    # 3. Calcular dados externos (dias úteis) antes de usar no with_columns
    bdays_to_payment = bday.count(settlement, df_cf.get_column("PaymentDate"))

    # 4. Construir o DataFrame final com todas as colunas necessárias
    df = df_cf.with_columns(BDToMat=bdays_to_payment).with_columns(
        BYears=pl.col("BDToMat") / 252,
        DIRate=pl.col("BDToMat").map_elements(ff_interpolator, return_dtype=pl.Float64),
    )

    # 5. Calcular o preço do título usando as taxas DI interpoladas
    bond_price = tools.calculate_present_value(
        cash_flows=df["CashFlow"],
        rates=df["DIRate"],
        periods=df["BYears"],
    )

    def price_difference(ytm: float) -> float:
        # A YTM que zera a diferença de preço
        discounted_cf = df["CashFlow"] / (1 + ytm) ** df["BYears"]
        return discounted_cf.sum() - bond_price

    # 7. Resolver para a YTM implícita
    di_ytm = _solve_spread(price_difference, ntnf_rate)

    if math.isnan(di_ytm):
        return float("nan")

    # 8. Calcular o prêmio final
    factor_ntnf = (1 + ntnf_rate) ** (1 / 252)
    factor_di = (1 + di_ytm) ** (1 / 252)

    # Evitar divisão por zero se o fator DI for 1
    if factor_di == 1:
        return float("inf") if factor_ntnf > 1 else 0.0

    premium_val = (factor_ntnf - 1) / (factor_di - 1)
    return premium_val


def duration(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float:
    """
    Calculate the Macaulay duration for an NTN-F bond in business years.

    Args:
        settlement (DateScalar): The settlement date to calculate the duration.
        maturity (DateScalar): The maturity date of the bond.
        rate (float): The yield to maturity (YTM) used to discount the cash flows.

    Returns:
        float: The Macaulay duration in business business years.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.duration("02-09-2024", "01-01-2035", 0.121785)
        6.32854218039796
    """
    if has_null_args(settlement, maturity, rate):
        return None
    # Normalize inputs
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    df = cash_flows(settlement, maturity)
    byears = bday.count(settlement, df["PaymentDate"]) / 252
    dcf = df["CashFlow"] / (1 + rate) ** byears
    duration = (dcf * byears).sum() / dcf.sum()
    return duration


def dv01(
    settlement: DateScalar,
    maturity: DateScalar,
    rate: float,
) -> float | None:
    """
    Calculate the DV01 (Dollar Value of 01) for an NTN-F in R$.

    Represents the price change in R$ for a 1 basis point (0.01%) increase in yield.

    Args:
        settlement (DateScalar): The settlement date in 'DD-MM-YYYY' format
            or a date-like object.
        maturity (DateScalar): The maturity date in 'DD-MM-YYYY' format or
            a date-like object.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-F.

    Returns:
        float | None: The DV01 value, representing the price change for a 1 basis point
            increase in yield.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.dv01("26-03-2025", "01-01-2035", 0.151375)
        0.39025200000003224
    """
    if has_null_args(settlement, maturity, rate):
        return None
    price1 = price(settlement, maturity, rate)
    price2 = price(settlement, maturity, rate + 0.0001)
    return price1 - price2


def di_spreads(date: DateScalar, bps: bool = False) -> pl.DataFrame:
    """
    Calcula o DI Spread para títulos prefixados (LTN e NTN-F) em uma data de referência.

    Definição do spread (forma bruta):
        DISpread_raw = IndicativeRate - SettlementRate

    Quando ``bps=False`` a coluna retorna essa diferença em formato decimal
    (ex: 0.000439 ≈ 4.39 bps). Quando ``bps=True`` o valor é automaticamente
    multiplicado por 10_000 e exibido diretamente em basis points.

    Args:
        date (DateScalar): Data de referência para buscar as taxas.
        bps (bool): Se True, retorna DISpread já convertido em basis points.
            Default False.

    Returns:
        pl.DataFrame com colunas:
            - BondType
            - MaturityDate
            - DISpread (decimal ou bps conforme parâmetro)

    Raises:
        ValueError: Se os dados de DI não possuem 'SettlementRate' ou estão vazios.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.di_spreads("30-05-2025", bps=True)
        shape: (5, 3)
        ┌──────────┬──────────────┬──────────┐
        │ BondType ┆ MaturityDate ┆ DISpread │
        │ ---      ┆ ---          ┆ ---      │
        │ str      ┆ date         ┆ f64      │
        ╞══════════╪══════════════╪══════════╡
        │ NTN-F    ┆ 2027-01-01   ┆ -3.31    │
        │ NTN-F    ┆ 2029-01-01   ┆ 14.21    │
        │ NTN-F    ┆ 2031-01-01   ┆ 21.61    │
        │ NTN-F    ┆ 2033-01-01   ┆ 11.51    │
        │ NTN-F    ┆ 2035-01-01   ┆ 22.0     │
        └──────────┴──────────────┴──────────┘
    """
    return pre_di_spreads(date, bps=bps).filter(pl.col("BondType") == "NTN-F")
