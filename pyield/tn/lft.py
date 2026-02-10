import polars as pl

from pyield import anbima, bday
from pyield._internal.types import DateLike, any_is_empty
from pyield.tn import tools


def data(date: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de LFT para a data de referência na ANBIMA.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de LFT.

    Output Columns:
        * BondType (String): Tipo do título (ex.: "LFT").
        * ReferenceDate (Date): Data de referência dos dados.
        * SelicCode (Int64): Código do título no SELIC.
        * IssueBaseDate (Date): Data base/emissão do título.
        * MaturityDate (Date): Data de vencimento do título.
        * BDToMat (Int64): Dias úteis entre referência e vencimento.
        * Duration (Float64): Macaulay Duration do título (anos).
        * DV01 (Float64): Variação no preço para 1bp de taxa.
        * DV01USD (Float64): DV01 convertido para USD pela PTAX do dia.
        * Price (Float64): Preço unitário (PU).
        * BidRate (Float64): Taxa de compra (decimal).
        * AskRate (Float64): Taxa de venda (decimal).
        * IndicativeRate (Float64): Taxa indicativa (decimal).
        * DIRate (Float64): Taxa DI interpolada (flat forward).
        * StdDev (Float64): Desvio padrão da taxa indicativa.
        * LowerBoundRateD0 (Float64): Limite inferior do intervalo (D+0).
        * UpperBoundRateD0 (Float64): Limite superior do intervalo (D+0).
        * LowerBoundRateD1 (Float64): Limite inferior do intervalo (D+1).
        * UpperBoundRateD1 (Float64): Limite superior do intervalo (D+1).
        * Criteria (String): Critério utilizado pela ANBIMA.

    Examples:
        >>> from pyield import lft
        >>> lft.data("23-08-2024")
        shape: (14, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬───────────┬───────────┬────────────────┬──────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate   ┆ AskRate   ┆ IndicativeRate ┆ DIRate   │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---       ┆ ---       ┆ ---            ┆ ---      │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64       ┆ f64       ┆ f64            ┆ f64      │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪═══════════╪═══════════╪════════════════╪══════════╡
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.000306  ┆ 0.000226  ┆ 0.000272       ┆ 0.10408  │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ -0.000397 ┆ -0.000481 ┆ -0.000418      ┆ 0.11082  │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ -0.000205 ┆ -0.000258 ┆ -0.00023       ┆ 0.114315 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.000085  ┆ 0.00006   ┆ 0.000075       ┆ 0.114982 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.000124  ┆ 0.000097  ┆ 0.000114       ┆ 0.114955 │
        │ …             ┆ …        ┆ …         ┆ …             ┆ … ┆ …         ┆ …         ┆ …              ┆ …        │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001501  ┆ 0.001476  ┆ 0.001491       ┆ 0.11564  │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001597  ┆ 0.001571  ┆ 0.001587       ┆ 0.115773 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001601  ┆ 0.001574  ┆ 0.001591       ┆ 0.115904 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001649  ┆ 0.001627  ┆ 0.001641       ┆ 0.115854 │
        │ 2024-08-23    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.001696  ┆ 0.00168   ┆ 0.001687       ┆ 0.115806 │
        └───────────────┴──────────┴───────────┴───────────────┴───┴───────────┴───────────┴────────────────┴──────────┘
    """  # noqa: E501
    return anbima.tpf_data(date, "LFT")


def maturities(date: DateLike) -> pl.Series:
    """
    Busca os vencimentos disponíveis para a data de referência.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.Series: Série de datas de vencimento disponíveis.

    Examples:
        >>> from pyield import lft
        >>> lft.maturities("22-08-2024")
        shape: (14,)
        Series: 'MaturityDate' [date]
        [
            2024-09-01
            2025-03-01
            2025-09-01
            2026-03-01
            2026-09-01
            …
            2029-03-01
            2029-09-01
            2030-03-01
            2030-06-01
            2030-09-01
        ]
    """
    return data(date)["MaturityDate"]


def quotation(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula a cotação de uma LFT pelas regras da ANBIMA.

    Args:
        settlement (DateLike): Data de liquidação do título.
        maturity (DateLike): Data de vencimento do título.
        rate (float): Taxa anualizada do título.

    Returns:
        float: Cotação do título.

    Examples:
        Calcula a cotação de uma LFT com taxa de 0,02:
        >>> from pyield import lft
        >>> lft.quotation(
        ...     settlement="24-07-2024",
        ...     maturity="01-09-2030",
        ...     rate=0.001717,  # 0.1717%
        ... )
        98.9645

        Entradas nulas retornam float('nan'):
        >>> lft.quotation(settlement=None, maturity="01-09-2030", rate=0.001717)
        nan
    """
    if any_is_empty(settlement, maturity, rate):
        return float("nan")
    # Número de dias úteis entre liquidação (inclusivo) e vencimento (exclusivo)
    dias_uteis = bday.count(settlement, maturity)

    # Número de períodos truncado conforme regras da ANBIMA
    anos_truncados = tools.truncate(dias_uteis / 252, 14)

    fator_desconto = 1 / (1 + rate) ** anos_truncados

    return tools.truncate(100 * fator_desconto, 4)


def premium(lft_rate: float, di_rate: float) -> float:
    """
    Calcula o prêmio da LFT sobre a taxa de DI Futuro.

    Args:
        lft_rate (float): Taxa anualizada da LFT sobre a Selic.
        di_rate (float): Taxa DI Futuro anualizada (interpolada para o mesmo
            vencimento da LFT).

    Returns:
        float: Prêmio da LFT sobre o DI.

    Examples:
        Calcula o prêmio de uma LFT em 28/04/2025:
        >>> from pyield import lft
        >>> lft_rate = 0.001124  # 0.1124%
        >>> di_rate = 0.13967670224373396  # 13.967670224373396%
        >>> lft.premium(lft_rate, di_rate)
        1.008594331960501
    """
    if any_is_empty(lft_rate, di_rate):
        return float("nan")
    # Taxa diária
    fator_lft = (lft_rate + 1) ** (1 / 252)
    fator_di = (di_rate + 1) ** (1 / 252)
    return (fator_lft * fator_di - 1) / (fator_di - 1)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calcula o preço da LFT pelas regras do Tesouro Nacional.

    Args:
        vna (float): Valor nominal atualizado (VNA).
        quotation (float): Cotação da LFT em base 100.
    Returns:
        float: Preço da LFT truncado em 6 casas decimais.

    References:
         - SEI Proccess 17944.005214/2024-09

    Examples:
        >>> from pyield import lft
        >>> lft.price(15785.324502, 99.9291)
        15774.132706
    """
    if any_is_empty(vna, quotation):
        return float("nan")
    return tools.truncate(vna * quotation / 100, 6)
