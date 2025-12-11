import logging
import math
from collections.abc import Callable

import pandas as pd
import polars as pl

import pyield.converters as cv
import pyield.interpolator as ip
from pyield import anbima, bday
from pyield.tn import tools
from pyield.tn.pre import di_spreads as pre_di_spreads
from pyield.types import ArrayLike, DateLike, has_nullable_args

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

logger = logging.getLogger(__name__)


def data(date: DateLike) -> pl.DataFrame:
    """
    Fetch the bond indicative rates for the given reference date.

    Args:
        date (DateLike): The reference date for fetching the data.

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


def maturities(date: DateLike) -> pl.Series:
    """
    Fetch the NTN-F bond maturities available for the given reference date.

    Args:
        date (DateLike): The reference date for fetching the data.

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
    settlement: DateLike,
    maturity: DateLike,
) -> pl.Series:
    """
    Generate all remaining coupon dates between a settlement date and a maturity date.
    The dates are exclusive for the settlement date and inclusive for the maturity date.
    Coupon payments are made on the 1st of January and July.
    The NTN-F bond is determined by its maturity date.

    Args:
        settlement (DateLike): The settlement date.
        maturity (DateLike): The maturity date.

    Returns:
        pl.Series: A Series containing the coupon dates between the settlement
            (exclusive) and maturity (inclusive) dates. Returns an empty Series if
            the maturity date is before or equal to the settlement date.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.payment_dates("15-05-2024", "01-01-2027")
        shape: (6,)
        Series: 'payment_dates' [date]
        [
            2024-07-01
            2025-01-01
            2025-07-01
            2026-01-01
            2026-07-01
            2027-01-01
        ]
    """
    if has_nullable_args(settlement, maturity):
        return pl.Series(dtype=pl.Date)
    # Normalize dates
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    # Check if maturity date is after the start date
    if maturity <= settlement:
        return pl.Series(dtype=pl.Date)

    # Initialize loop variables
    coupon_date = maturity
    coupon_dates = []

    # Iterate backwards from the maturity date to the settlement date
    while coupon_date > settlement:
        coupon_dates.append(coupon_date)
        # Move the coupon date back 6 months
        coupon_date -= pd.DateOffset(months=6)
        coupon_date = coupon_date.date()  # DateOffset returns a Timestamp

    return pl.Series(name="payment_dates", values=coupon_dates).sort()


def cash_flows(
    settlement: DateLike,
    maturity: DateLike,
    adj_payment_dates: bool = False,
) -> pl.DataFrame:
    """
    Generate the cash flows for the NTN-F bond between the settlement (exclusive) and
    maturity dates (inclusive). The cash flows are the coupon payments and the final
    payment at maturity.

    Args:
        settlement (DateLike): The date (exclusive) for starting the cash flows.
        maturity (DateLike): The maturity date of the bond.
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
    if has_nullable_args(settlement, maturity):
        return pl.DataFrame()
    # Normalize input dates
    settlement = cv.convert_dates(settlement)
    maturity = cv.convert_dates(maturity)

    # Get the payment dates between the settlement and maturity dates
    pmt_dates = payment_dates(settlement, maturity)

    # Return empty DataFrame if no payment dates (settlement >= maturity)
    if pmt_dates.is_empty():
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    # Set the cash flow at maturity to FINAL_PMT and the others to COUPON_PMT
    df = pl.DataFrame(
        data={"PaymentDate": pmt_dates},
    ).with_columns(
        pl.when(pl.col("PaymentDate") == maturity)
        .then(FINAL_PMT)
        .otherwise(COUPON_PMT)
        .alias("CashFlow")
    )

    if adj_payment_dates:
        adj_pay_dates = bday.offset(pmt_dates, 0)
        df = df.with_columns(PaymentDate=adj_pay_dates)
    return df


def price(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calculate the NTN-F price using Anbima rules, which corresponds to the present
        value of the cash flows discounted at the given yield to maturity rate (YTM).

    Args:
        settlement (DateLike): The settlement date to calculate the price.
        maturity (DateLike): The maturity date of the bond.
        rate (float): The discount rate (yield to maturity) used to calculate the
            present value of the cash flows.

    Returns:
        float: The NTN-F price using Anbima rules.

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
    if has_nullable_args(settlement, maturity, rate):
        return float("nan")

    df_cf = cash_flows(settlement, maturity)
    if df_cf.is_empty():
        return float("nan")

    cf_values = df_cf["CashFlow"]
    bdays = bday.count(settlement, df_cf["PaymentDate"])
    byears = tools.truncate(bdays / 252, 14)
    discount_factors = (1 + rate) ** byears
    # Calculate the present value of each cash flow (DCF) rounded as per Anbima rules
    dcf = (cf_values / discount_factors).round(9)
    # Return the sum of the discounted cash flows truncated as per Anbima rules
    return tools.truncate(dcf.sum(), 6)


def spot_rates(  # noqa
    settlement: DateLike,
    ltn_maturities: ArrayLike,
    ltn_rates: ArrayLike,
    ntnf_maturities: ArrayLike,
    ntnf_rates: ArrayLike,
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
        settlement (DateLike): The settlement date for the spot rates calculation.
        ltn_maturities (ArrayLike): The LTN known maturities.
        ltn_rates (ArrayLike): The LTN known rates.
        ntnf_maturities (ArrayLike): The NTN-F known maturities.
        ntnf_rates (ArrayLike): The NTN-F known rates.
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
    if has_nullable_args(
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
    ytm_rates = ntnf_rate_interpolator(bdays_to_mat)
    df = pl.DataFrame(
        {
            "MaturityDate": all_coupon_dates,
            "BDToMat": bdays_to_mat,
            "BYears": bdays_to_mat / 252,
            "YTM": ytm_rates,
        }
    ).with_columns(
        Coupon=pl.lit(COUPON_PMT),
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


def _find_bracketing_interval(
    func: Callable[[float], float],
) -> tuple[float, float] | None:
    """
    Encontra um intervalo [a, b] para a TAXA DE JUROS que zera a função.

    Otimizado para o contexto financeiro, buscando a taxa apenas em um
    intervalo realista. A função 'func' é a que calcula a diferença de
    preço dado uma taxa.
    """
    # --- LIMITES DE BOM SENSO PARA A *TAXA* QUE ESTAMOS PROCURANDO ---
    # Uma taxa/spread não vai ser -50% ou +200%, então limitamos a busca.
    X0: float = 0.01
    STEP: float = 0.01
    GROWTH_FACTOR: float = 1.6
    MAX_ATTEMPTS: int = 100

    # Limites para a TAXA (variável 'a' e 'b' da busca)
    MIN_RATE: float = -1.0  # Limite inferior: -100%
    MAX_RATE: float = 10.00  # Limite superior: 1000%
    # -----------------------------------------------------------------

    # Ponto de partida
    f0 = func(X0)
    if abs(f0) == 0:
        return (X0, X0)

    # 1. Busca na direção positiva
    a, fa = X0, f0
    b = X0 + STEP
    current_step = STEP

    for _ in range(MAX_ATTEMPTS):
        # Se a PRÓXIMA TAXA A SER TESTADA ('b') for irrealista, paramos.
        if b > MAX_RATE:
            break

        fb = func(b)
        if fa * fb < 0:
            return (a, b)

        a, fa = b, fb
        current_step *= GROWTH_FACTOR
        b += current_step

    # 2. Busca na direção negativa
    a, fa = X0, f0
    b = X0 - STEP
    current_step = STEP

    for _ in range(MAX_ATTEMPTS):
        # Se a PRÓXIMA TAXA A SER TESTADA ('b') for irrealista, paramos.
        if b < MIN_RATE:
            break

        fb = func(b)
        if fa * fb < 0:
            return (b, a)

        a, fa = b, fb
        current_step *= GROWTH_FACTOR
        b -= current_step

    # Se a busca falhou dentro dos limites realistas
    return None


def _bisection_method(func: Callable[[float], float], a: float, b: float) -> float:
    """Bisection method for root finding.

    Args:
        func (Callable[[float], float]): Function for which the root is sought. Must
            accept a single float and return a float.
        a (float): Lower bound of the interval.
        b (float): Upper bound of the interval.

    Returns:
        float: Approximate root of ``func`` within the interval ``[a, b]``.

    Raises:
        ValueError: If ``func`` does not change sign in the interval ``[a, b]``.
    """
    TOL = 1e-8
    MAX_ITER = 100
    fa, fb = func(a), func(b)
    if fa * fb > 0:
        logger.warning(
            "Bisection method failed: function does not change sign in the interval."
        )
        return float("nan")

    for _ in range(MAX_ITER):
        midpoint = (a + b) / 2
        fmid = func(midpoint)
        if abs(fmid) < TOL or (b - a) / 2 < TOL:
            return midpoint
        if fmid * fa < 0:
            b, fb = midpoint, fmid
        else:
            a, fa = midpoint, fmid

    return (a + b) / 2


def _solve_spread(
    price_difference_func: Callable,
) -> float:
    """
    Versão robusta que encontra automaticamente um intervalo válido.
    """
    # Tenta encontrar intervalo válido
    bracket = _find_bracketing_interval(price_difference_func)

    if bracket is None:
        logger.warning("Não foi possível encontrar intervalo de busca válido")
        return float("nan")

    a, b = bracket
    return _bisection_method(price_difference_func, a, b)


def premium(  # noqa
    settlement: DateLike,
    ntnf_maturity: DateLike,
    ntnf_rate: float,
    di_expirations: DateLike,
    di_rates: ArrayLike,
) -> float:
    """
    Calculate the premium of an NTN-F bond over DI rates.

    This function computes the premium of an NTN-F bond by comparing its implied
    discount factor with that of the DI curve. It determines the net premium based
    on the difference between the discount factors of the bond's yield-to-maturity
    (YTM) and the interpolated DI rates.

    Args:
        settlement (DateLike): The settlement date to calculate the premium.
        ntnf_maturity (DateLike): The maturity date of the NTN-F bond.
        ntnf_rate (float): The yield to maturity (YTM) of the NTN-F bond.
        di_expirations (DateLike): Series with the expiration dates for the DI.
        di_rates (ArrayLike): Series containing the DI rates corresponding to
            the expiration dates.

    Returns:
        float: The premium of the NTN-F bond over the DI curve, expressed as a
        factor. If calculation fails, returns NaN.

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
        1.0099602679927115

    Notes:
        - The function adjusts coupon payment dates to business days and calculates
          the present value of cash flows for the NTN-F bond using DI rates.

    """
    if has_nullable_args(
        settlement, ntnf_maturity, ntnf_rate, di_expirations, di_rates
    ):
        return float("nan")

    if not isinstance(di_rates, pl.Series):
        di_rates = pl.Series(di_rates)

    df_cf = cash_flows(settlement, ntnf_maturity, adj_payment_dates=True)
    if df_cf.is_empty():
        return float("nan")

    ff_interpolator = ip.Interpolator(
        "flat_forward",
        bday.count(settlement, di_expirations),
        di_rates,
    )

    bdays_to_payments = bday.count(settlement, df_cf["PaymentDate"])
    df = df_cf.with_columns(
        BDToMat=bdays_to_payments,
        BYears=bdays_to_payments / 252,
        DIRate=ff_interpolator(bdays_to_payments),
    )

    bond_price = tools.calculate_present_value(
        cash_flows=df["CashFlow"],
        rates=df["DIRate"],
        periods=df["BYears"],
    )

    if math.isnan(bond_price):
        return float("nan")

    def price_difference(rate: float) -> float:
        discounted_cf = df["CashFlow"] / (1 + rate) ** df["BYears"]
        return discounted_cf.sum() - bond_price

    di_ytm = _solve_spread(price_difference)

    if math.isnan(di_ytm):
        return float("nan")

    factor_ntnf = (1 + ntnf_rate) ** (1 / 252)
    factor_di = (1 + di_ytm) ** (1 / 252)
    if factor_di == 1:
        return float("inf") if factor_ntnf > 1 else 0.0

    premium_val = (factor_ntnf - 1) / (factor_di - 1)
    return premium_val


def di_net_spread(  # noqa
    settlement: DateLike,
    ntnf_maturity: DateLike,
    ntnf_rate: float,
    di_expirations: ArrayLike,
    di_rates: ArrayLike,
) -> float:
    """
    Calculate the net DI spread for a bond given the YTM and the DI rates.

    This function determines the spread over the DI curve that equates the present value
    of the bond's cash flows to its market price. It interpolates the DI rates to match
    the bond's cash flow payment dates and uses the Brent method to find the spread
    (in bps) that zeroes the difference between the bond's market price and its
    discounted cash flows.

    Args:
        settlement (DateLike): The settlement date to calculate the spread.
        ntnf_maturity (DateLike): The bond maturity date.
        ntnf_rate (float): The yield to maturity (YTM) of the bond.
        di_rates (ArrayLike): A Series of DI rates.
        di_expirations (ArrayLike): A list or Series of DI expiration dates.

    Returns:
        float: The net DI spread in decimal format (e.g., 0.0012 for 12 bps).
            If calculation fails, returns NaN.

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

        >>> # Nullable inputs return float('nan')
        >>> di_net_spread(
        ...     settlement=None,
        ...     ntnf_maturity="01-01-2035",
        ...     ntnf_rate=0.116586,
        ...     di_expirations=exp_dates,
        ...     di_rates=di_rates,
        ... )
        nan
    """
    # Validação de inputs
    if has_nullable_args(
        settlement, ntnf_maturity, ntnf_rate, di_expirations, di_rates
    ):
        return float("nan")

    # Force di_rates to be a Polars Series
    if not isinstance(di_rates, pl.Series):
        di_rates = pl.Series(di_rates)

    # Criação do interpolador
    ff_interpolator = ip.Interpolator(
        "flat_forward",
        bday.count(settlement, di_expirations),
        di_rates,
    )

    # Geração dos fluxos de caixa do NTN-F
    df = cash_flows(settlement, ntnf_maturity)
    if df.is_empty():
        return float("nan")

    bdays_to_payment = bday.count(settlement, df["PaymentDate"])
    byears_to_payment = bdays_to_payment / 252

    df = df.with_columns(
        BDaysToPayment=bdays_to_payment,
        DIRateInterp=ff_interpolator(bdays_to_payment),
    )

    # Extração dos dados para o cálculo numérico
    bond_price = price(settlement, ntnf_maturity, ntnf_rate)
    bond_cash_flows = df["CashFlow"]
    di_interp = df["DIRateInterp"]

    # Função de diferença de preço para o solver
    def price_difference(p: float) -> float:
        discounted_cf = bond_cash_flows / (1 + di_interp + p) ** byears_to_payment
        return discounted_cf.sum() - bond_price

    # 7. Resolver para o spread
    return _solve_spread(price_difference)


def duration(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calculate the Macaulay duration for an NTN-F bond in business years.

    Args:
        settlement (DateLike): The settlement date to calculate the duration.
        maturity (DateLike): The maturity date of the bond.
        rate (float): The yield to maturity (YTM) used to discount the cash flows.

    Returns:
        float: The Macaulay duration in business business years. Returns NaN if
            calculation is not possible due to invalid inputs.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.duration("02-09-2024", "01-01-2035", 0.121785)
        6.32854218039796

        Nullable inputs return NaN:
        >>> ntnf.duration(None, "01-01-2035", 0.121785)
        nan
    """
    if has_nullable_args(settlement, maturity, rate):
        return float("nan")

    df = cash_flows(settlement, maturity)
    if df.is_empty():
        return float("nan")

    byears = bday.count(settlement, df["PaymentDate"]) / 252
    dcf = df["CashFlow"] / (1 + rate) ** byears
    duration = (dcf * byears).sum() / dcf.sum()
    return duration


def dv01(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calculate the DV01 (Dollar Value of 01) for an NTN-F in R$.

    Represents the price change in R$ for a 1 basis point (0.01%) increase in yield.

    Args:
        settlement (DateLike): The settlement date in 'DD-MM-YYYY' format
            or a date-like object.
        maturity (DateLike): The maturity date in 'DD-MM-YYYY' format or
            a date-like object.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-F.

    Returns:
        float: The DV01 value, representing the price change for a 1 basis point
            increase in yield.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.dv01("26-03-2025", "01-01-2035", 0.151375)
        0.39025200000003224

        Nullable inputs return NaN:
        >>> ntnf.dv01("", "01-01-2035", 0.151375)
        nan
    """
    if has_nullable_args(settlement, maturity, rate):
        return float("nan")

    price1 = price(settlement, maturity, rate)
    price2 = price(settlement, maturity, rate + 0.0001)
    return price1 - price2


def di_spreads(date: DateLike, bps: bool = False) -> pl.DataFrame:
    """
    Calcula o DI Spread para títulos prefixados (LTN e NTN-F) em uma data de referência.

    Definição do spread (forma bruta):
        DISpread_raw = IndicativeRate - SettlementRate

    Quando ``bps=False`` a coluna retorna essa diferença em formato decimal
    (ex: 0.000439 ≈ 4.39 bps). Quando ``bps=True`` o valor é automaticamente
    multiplicado por 10_000 e exibido diretamente em basis points.

    Args:
        date (DateLike): Data de referência para buscar as taxas.
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
