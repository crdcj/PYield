"""
probabilidades — Probabilidades implícitas das reuniões do COPOM a partir dos
preços das opções CPM.

O contrato CPM é uma opção europeia cash-or-nothing. No apreçamento neutro ao
risco, o preço de ajuste da B3 em pontos (0–100) representa a probabilidade
implícita de cada cenário de alteração da Selic, descontada pela taxa DI até o
vencimento (Manual de Apreçamento da B3, §3.5).

Convenções de probabilidade
---------------------------
    prob_bruta = preco_ajuste * fator_desconto / 100

            Probabilidade neutra ao risco direta segundo o Manual da B3 §3.5
            (fórmula invertida):

          p_n(K) = PR_n * exp(+n * r_n) / N

      onde
          PR_n  = preco_ajuste ("Preço de Referência" da B3, pontos 0–100)
          N     = 100 (valor nocional fixo)
          n     = dias_uteis / 252 (tempo em anos, convenção de dias úteis)
          r_n   = ln(1 + taxa_di1) (taxa DI1 continuamente capitalizada até o
                  vencimento)
          taxa_di1 = taxa DI1 interpolada por flat-forward entre a data de
                     referência e a data de expiração

      O preco_ajuste de cpm.data() é o "Preço de Referência" oficial da B3,
      obtido no endpoint CSV: o preço exibido no painel da B3 ("Probabilidades
      da Taxa Selic Meta") e resultado da metodologia P1/P2/P3/P4 da B3. Pode
      ser nulo para datas com mais de aproximadamente um mês, quando o
      endpoint CSV não está disponível.

      Pode não somar 1,0 por reunião devido ao spread entre oferta e demanda ou
      ao apreçamento P1/P2 da B3. Quando dias_uteis == 0 (o próprio dia da
      reunião), fator_desconto == 1,0 exatamente e prob_bruta se reduz a
      preco_ajuste / 100.

  prob = prob_bruta / soma(prob_bruta) no grupo de data_expiracao
      Normalizada para que cada reunião some exatamente 1,0. Este é o ajuste de
      apreçamento P3 da B3. Use esta coluna para análise de cenários e gráficos.

  prob_acumulada = soma acumulada de prob, ordenada por
      variacao_strike_bps em ordem crescente.

Observações sobre preco_ajuste nulo
-----------------------------------
Às vezes os contratos CPM são listados sem preço de ajuste (não há apreçamento
oficial da B3 para aquele strike na data, ou o endpoint CSV não está disponível
para datas antigas). Strikes com preco_ajuste nulo são excluídos da saída de
probabilidades porque:

  1. Sua contribuição para a distribuição normalizada é indefinida.
  2. ``group_by().agg(sum())`` do Polars retorna 0,0 (e não nulo) para grupos
     totalmente nulos, o que quebraria a invariável ``prob.sum() == 1.0`` por
     reunião.

Como consequência, uma reunião em que TODOS os strikes listados tenham preços
nulos (por exemplo, CPMH25 no dia da reunião do COPOM de janeiro de 2025) não
aparecerá na saída. O ranking_reuniao é, portanto, atribuído somente entre as
reuniões com preços e sempre forma uma sequência consecutiva [1, 2, ..., n].

Observações sobre o fallback do DI1
-----------------------------------
Se os dados do DI1 não estiverem disponíveis para a data de referência (erro de
rede, feriado etc.), taxa_di1 assume 0,0 e fator_desconto assume 1,0. Assim,
prob_bruta se reduz a preco_ajuste / 100, equivalente à fórmula antiga
(incorreta). Essa degradação é registrada como aviso, mas nunca gera uma
exceção.
"""

import logging

import polars as pl

from pyield import cpm
from pyield._internal.converters import converter_datas
from pyield._internal.types import DateLike
from pyield.futuro import di1

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

    Uses the vectorized di1.interpolar_taxas() to fetch all DI1 rates in a
    single call (one data fetch + one interpolador per unique TradeDate),
    then computes discount factors with Polars expressions.

    Interpolation method
    --------------------
    di1.interpolar_taxas() implements B3 Manual §1.4.2 — Flat Forward 252,
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
        rates = di1.interpolar_taxas(
            datas_referencia=pairs["data_referencia"],
            datas_vencimento=pairs["data_expiracao"],
            extrapolar=True,
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
        df.sort("data_expiracao", "variacao_strike_bps")
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
    Calcula as probabilidades implícitas de todas as reuniões do COPOM com
    contratos CPM negociados na data informada em `date`.

    Somente strikes com `preco_ajuste` não nulo são incluídos. Reuniões em que
    todos os strikes listados têm preço nulo são excluídas integralmente (ver
    as observações no nível do módulo).

    Args:
        date: Data de referência da consulta; aceita os formatos de `DateLike`.
        option_type: Tipo de opção, `"call"` ou `"put"`. O padrão é `"call"`,
            que na prática é o lado mais líquido.

    Returns:
        `polars.DataFrame` ordenado por `ranking_reuniao` e
        `variacao_strike_bps`. Em caso de ausência de dados, retorna um
        DataFrame vazio com o schema correto.

    Output Columns:
        - data_referencia (Date): Data de negociação.
        - data_fim_reuniao (Date): Último dia da reunião do COPOM.
        - data_expiracao (Date): Data de expiração do contrato CPM.
        - ranking_reuniao (Int32): 1 para a reunião mais próxima com preços.
        - variacao_strike_bps (Int32): Variação do strike em pontos-base,
            crescente dentro de cada reunião.
        - dias_uteis (Int32): Dias úteis até a expiração.
        - preco_ajuste (Float64): Preço de Referência da B3 em pontos (0–100).
        - taxa_di1 (Float64): Taxa DI1 interpolada até a expiração.
        - fator_desconto (Float64): Fator aplicado na inversão da fórmula da B3,
            calculado como `exp(dias_uteis / 252 * ln(1 + taxa_di1))`.
        - prob_bruta (Float64): Probabilidade antes da normalização,
            calculada como `preco_ajuste * fator_desconto / 100`.
        - prob (Float64): Probabilidade normalizada, calculada como
            `prob_bruta / soma(prob_bruta)` por `data_expiracao`, com soma
            1,0 por reunião.
        - prob_acumulada (Float64): Probabilidade acumulada em ordem
            crescente de strike.

    Examples:
        >>> import pyield as yd
        >>> import polars as pl
        >>> df = yd.cpm.probabilidades.all_meetings("29-01-2025")  # doctest: +SKIP
        >>> df.is_empty() or df["ranking_reuniao"].min() == 1  # doctest: +SKIP
        True
        >>> sums = df.group_by("data_expiracao").agg(
        ...     pl.col("prob").sum()
        ... )  # doctest: +SKIP
        >>> df.is_empty() or (sums["prob"] - 1.0).abs().max() < 1e-9  # doctest: +SKIP
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
        .sort("ranking_reuniao", "variacao_strike_bps")
    )

    return df if not df.is_empty() else _empty_schema()


def meeting(
    date: DateLike,
    expiration: DateLike | None = None,
    option_type: str = "call",
) -> pl.DataFrame:
    """
    Calcula as probabilidades implícitas de uma única reunião do COPOM.

    Args:
        date: Data de referência da consulta.
        expiration: Data de expiração do contrato CPM da reunião desejada, ou
            `None` para selecionar a reunião mais próxima com preços. A expiração
            corresponde ao primeiro dia útil após o término da reunião.
        option_type: Tipo de opção, `"call"` ou `"put"`. O padrão é `"call"`.

    Returns:
        `polars.DataFrame` com o mesmo schema de `all_meetings()`, filtrado
        para uma única reunião. `ranking_reuniao` é sempre 1 nesta saída,
        relativo à reunião selecionada, e não ao conjunto de reuniões.

    Examples:
        >>> import pyield as yd
        >>> df = yd.cpm.probabilidades.meeting("29-01-2025")  # doctest: +SKIP
        >>> df.is_empty() or abs(df["prob"].sum() - 1.0) < 1e-9  # doctest: +SKIP
        True
        >>> df.is_empty() or df["prob_acumulada"].tail(
        ...     1
        ... ).item() == 1.0  # doctest: +SKIP
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
