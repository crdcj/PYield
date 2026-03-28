"""
probabilities — Implied COPOM meeting probabilities from CPM option prices.

The CPM contract is a cash-or-nothing European option.  Under risk-neutral
pricing, the B3 settlement price in points (0–100) encodes the market-implied
probability of each Selic change scenario, discounted by the DI rate to expiry
(B3 Pricing Manual §3.5).

Probability conventions
------------------------
  RawProb  = SettlementPrice * DiscountExp / 100

      Direct risk-neutral probability per B3 Manual §3.5 (inverted):

          p_n(K) = PR_n * exp(+n * r_n) / N

      where
          PR_n  = SettlementPrice  (B3 "Preço de Referência", points 0–100)
          N     = 100              (fixed notional)
          n     = BDaysToExp / 252 (time in years, business-day convention)
          r_n   = ln(1 + DI1Rate)  (continuously-compounded DI1 rate to expiry)
          DI1Rate = flat-forward interpolated DI1 rate from TradeDate to ExpiryDate

      SettlementPrice in cpm.data() is the B3 official "Preço de Referência"
      from the B3 CSV endpoint — the price shown on the B3 dashboard
      ("Probabilidades da Taxa Selic Meta") and the output of B3's
      P1/P2/P3/P4 methodology.  It may be null for dates older than ~1 month
      where the CSV endpoint is unavailable.

      May not sum to 1.0 per meeting due to bid-ask spreads or B3 P1/P2 pricing.
      When BDaysToExp == 0 (meeting-day itself), DiscountExp == 1.0 exactly and
      RawProb reduces to SettlementPrice / 100.

  Prob  = RawProb / sum(RawProb) within ExpiryDate group
      Normalized so each meeting sums to exactly 1.0.  This is the B3 P3
      pricing adjustment.  Use this for scenario analysis and charts.

  CumProb = cumulative sum of Prob, sorted by StrikeChangeBps ascending.

Notes on null SettlementPrice
------------------------------
CPM contracts are sometimes listed without a settlement price (no B3 official
pricing for that strike on that date, or CSV endpoint unavailable for older
dates).  Strikes with null SettlementPrice are excluded from the probability
output because:

  1. Their contribution to the normalized distribution is undefined.
  2. Polars ``group_by().agg(sum())`` returns 0.0 (not null) for all-null
     groups, which would break the invariant ``Prob.sum() == 1.0`` per meeting.

As a consequence, a meeting where ALL listed strikes have null prices (e.g.
CPMH25 on the January 2025 COPOM day) will not appear in the output.
MeetingRank is therefore assigned over the priced meetings only and is always
a consecutive sequence [1, 2, …, n].

Notes on DI1 fallback
----------------------
If DI1 data is unavailable for the trade date (network error, holiday, etc.),
DI1Rate falls back to 0.0 and DiscountExp to 1.0, so RawProb reduces to
SettlementPrice / 100 — equivalent to the old (incorrect) formula.  This
degradation is logged as a warning but never raises an exception.
"""

from __future__ import annotations

import logging

import polars as pl

from pyield._internal.converters import converter_datas
from pyield._internal.types import DateLike
from pyield.b3 import di1
from pyield.selic import cpm

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _empty_schema() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "data_referencia": pl.Date,
            "data_fim_reuniao": pl.Date,
            "data_expiracao": pl.Date,
            "ranking_reuniao": pl.Int32,
            "variacao_strike_bps": pl.Int32,
            "dias_uteis": pl.Int32,
            "preco_ajuste": pl.Float64,
            "taxa_di1": pl.Float64,
            "fator_desconto": pl.Float64,
            "prob_bruta": pl.Float64,
            "prob": pl.Float64,
            "prob_acumulada": pl.Float64,
        }
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _add_meeting_rank(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona ranking_reuniao: 1 = data_expiracao mais próxima, 2 = seguinte, etc.
    Calculado como dense rank sobre data_expiracao.
    """
    return df.with_columns(
        ranking_reuniao=pl.col("data_expiracao").rank("dense").cast(pl.Int32)
    )


def _add_discount_factors(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add columns:
        DI1Rate      : Float64  DI1 rate interpolated to ExpiryDate
        DiscountExp  : Float64  exp(+n * r_n), the B3 pricing discount factor
                                where n = BDaysToExp/252, r_n = ln(1+DI1Rate)

    Uses the vectorized di1.interpolate_rates() to fetch all DI1 rates in a
    single call (one data fetch + one interpolator per unique TradeDate),
    then computes discount factors with Polars expressions.

    Interpolation method
    --------------------
    di1.interpolate_rates() implements B3 Manual §1.4.2 — Flat Forward 252,
    which log-linearly interpolates accumulated DI1 price factors (PU values):

        fa_j = (1 + r_j)^(du_j/252)          # accumulated factor at node j
        fa_k = (1 + r_k)^(du_k/252)          # accumulated factor at node k
        ft   = (du - du_j) / (du_k - du_j)   # time fraction
        r    = (fa_j * (fa_k / fa_j)^ft)^(252/du) - 1

    This is equivalent to log-linear interpolation of DI1 settlement prices
    (PU = 100_000 / (1+r)^(du/252)) — hence "interpolação exponencial dos
    preços de ajuste do DI1" in the CPM Pricing Manual §3.5.

    It is NOT §1.4.1 (Exponencial 252), which interpolates rates directly:
        r = (1 + r_j) * ((1 + r_k)/(1 + r_j))^ft - 1
    The two methods diverge by several basis points at intermediate maturities
    (e.g. ~4.6 bps difference at du=17 for typical Selic-range rates in 2026).

    Falls back to DI1Rate=0.0 / DiscountExp=1.0 when DI1 data is unavailable.
    """
    pairs = (
        df.select("data_referencia", "data_expiracao", "dias_uteis")
        .unique(subset=["data_referencia", "data_expiracao"])
        .sort("data_referencia", "data_expiracao")
    )

    # Chamada vetorizada: um fetch por data_referencia única
    try:
        rates = di1.interpolate_rates(
            dates=pairs["data_referencia"],
            expirations=pairs["data_expiracao"],
            extrapolate=True,
        )
    except Exception:
        logger.warning("Falha na busca DI1; usando fallback taxa=0.0.")
        rates = pl.Series("taxa_interpolada", [None] * len(pairs), dtype=pl.Float64)

    discount_df = (
        pairs.with_columns(
            taxa_di1=rates.fill_null(0.0).fill_nan(0.0),
        )
        .with_columns(
            fator_desconto=(
                (pl.col("dias_uteis") / 252 * (1 + pl.col("taxa_di1")).log()).exp()
            ),
        )
        .select("data_referencia", "data_expiracao", "taxa_di1", "fator_desconto")
    )

    return df.join(discount_df, on=["data_referencia", "data_expiracao"], how="left")


def _add_probabilities(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add RawProb, Prob, and CumProb columns per B3 Manual §3.5.

    Assumes df has already been filtered to one option_type and to rows
    with non-null SettlementPrice, and that _add_discount_factors has
    been called so DI1Rate and DiscountExp are present.

    RawProb  = SettlementPrice * DiscountExp / 100
    Prob     = RawProb / sum(RawProb) within ExpiryDate group
    CumProb  = cumulative sum of Prob, sorted by StrikeChangeBps ascending
    """
    df = _add_discount_factors(df)

    return (
        df.sort(["data_expiracao", "variacao_strike_bps"])
        .with_columns(
            prob_bruta=(pl.col("preco_ajuste") * pl.col("fator_desconto") / 100),
        )
        .with_columns(
            prob=(
                pl.col("prob_bruta") / pl.col("prob_bruta").sum().over("data_expiracao")
            ),
        )
        .with_columns(prob_acumulada=pl.col("prob").cum_sum().over("data_expiracao"))
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def all_meetings(
    date: DateLike,
    option_type: str = "call",
) -> pl.DataFrame:
    """
    Implied COPOM probabilities for every meeting with CPM contracts
    trading on `date`.

    Only strikes with a non-null SettlementPrice are included.  Meetings
    where all listed strikes have null prices are excluded entirely (see
    module-level notes).

    Parameters
    ----------
    date : DateLike
        Trade date.
    option_type : {"call", "put"}
        Which side to use. Default "call" (the liquid side in practice).

    Returns
    -------
    pl.DataFrame
        Columns:
            TradeDate      : Date
            MeetingEndDate : Date
            ExpiryDate     : Date
            MeetingRank    : Int32   1 = nearest meeting with priced contracts
            StrikeChangeBps: Int32   sorted ascending within each meeting
            BDaysToExp     : Int32   business days from TradeDate to ExpiryDate
            SettlementPrice: Float64 B3 "Preço de Referência" in points (0–100)
            DI1Rate        : Float64 flat-forward DI1 rate to ExpiryDate
            DiscountExp    : Float64 exp(BDaysToExp/252 * ln(1+DI1Rate))
            RawProb        : Float64 SettlementPrice * DiscountExp / 100
            Prob           : Float64 normalized, sums to 1.0 per meeting
            CumProb        : Float64 cumulative Prob ascending by strike

        Sorted by (MeetingRank, StrikeChangeBps).
        Returns empty DataFrame with correct schema on missing data.

    Examples
    --------
    >>> import pyield as yd
    >>> import polars as pl
    >>> df = yd.selic.probabilities.all_meetings("29-01-2025")
    >>> df.is_empty() or df["ranking_reuniao"].min() == 1
    True
    >>> sums = df.group_by("data_expiracao").agg(pl.col("prob").sum())
    >>> df.is_empty() or (sums["prob"] - 1.0).abs().max() < 1e-9
    True
    """
    raw = cpm.data(date)
    if raw.is_empty():
        return _empty_schema()

    df = (
        raw.filter(pl.col("tipo_opcao") == option_type)
        # Excluir strikes sem preço de ajuste — ver docstring do módulo.
        .filter(pl.col("preco_ajuste").is_not_null())
        .pipe(_add_meeting_rank)
        .pipe(_add_probabilities)
        .select(
            "data_referencia",
            "data_fim_reuniao",
            "data_expiracao",
            "ranking_reuniao",
            "variacao_strike_bps",
            "dias_uteis",
            "preco_ajuste",
            "taxa_di1",
            "fator_desconto",
            "prob_bruta",
            "prob",
            "prob_acumulada",
        )
        .sort(["ranking_reuniao", "variacao_strike_bps"])
    )

    return df if not df.is_empty() else _empty_schema()


def meeting(
    date: DateLike,
    expiration: DateLike | None = None,
    option_type: str = "call",
) -> pl.DataFrame:
    """
    Implied COPOM probabilities for a single meeting.

    Parameters
    ----------
    date : DateLike
        Trade date.
    expiration : DateLike | None
        ExpiryDate of the target meeting (B3 contract expiry date,
        i.e. next business day after the meeting end).
        If None, the nearest meeting with priced contracts is used.
    option_type : {"call", "put"}
        Which side to use. Default "call".

    Returns
    -------
    pl.DataFrame
        Same schema as all_meetings(), filtered to the single meeting.
        MeetingRank is always 1 in this output (relative to the
        selected meeting — do not confuse with rank across all meetings).

    Examples
    --------
    >>> import pyield as yd
    >>> df = yd.selic.probabilities.meeting("29-01-2025")
    >>> df.is_empty() or abs(df["prob"].sum() - 1.0) < 1e-9
    True
    >>> df.is_empty() or df["prob_acumulada"].tail(1).item() == 1.0
    True
    """
    df = all_meetings(date, option_type=option_type)
    if df.is_empty():
        return _empty_schema()

    if expiration is None:
        target_expiry = df.filter(pl.col("ranking_reuniao") == 1)["data_expiracao"][0]
    else:
        target_expiry = converter_datas(expiration)

    return df.filter(pl.col("data_expiracao") == target_expiry).with_columns(
        ranking_reuniao=pl.lit(1, dtype=pl.Int32)
    )
