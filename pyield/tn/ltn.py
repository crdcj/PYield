import polars as pl

from pyield import anbima, bday, fwd
from pyield._internal.types import DateLike, any_is_empty
from pyield.tn import utils
from pyield.tn.pre import di_spreads as pre_di_spreads

VALOR_FACE = 1000


def data(date: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de LTN na ANBIMA para a data de referência.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de LTN.

    Output Columns:
        - BondType (String): Tipo do título (ex.: "LTN").
        - ReferenceDate (Date): Data de referência dos dados.
        - SelicCode (Int64): Código do título no SELIC.
        - IssueBaseDate (Date): Data base/emissão do título.
        - MaturityDate (Date): Data de vencimento do título.
        - BDToMat (Int64): Dias úteis entre referência e vencimento.
        - Duration (Float64): Macaulay Duration do título (anos).
        - AvgMaturity (Float64): Prazo médio do título (anos).
        - DV01 (Float64): Variação no preço para 1bp de taxa.
        - DV01USD (Float64): DV01 convertido para USD pela PTAX do dia.
        - Price (Float64): Preço unitário (PU).
        - BidRate (Float64): Taxa de compra (decimal).
        - AskRate (Float64): Taxa de venda (decimal).
        - IndicativeRate (Float64): Taxa indicativa (decimal).
        - DIRate (Float64): Taxa DI interpolada (flat forward).
        - DISpread (Float64): Spread sobre o DI (IndicativeRate - DIRate).
        - Premium (Float64): Rentabilidade diária da LTN sobre o DI.

    Examples:
        >>> from pyield import ltn
        >>> df_ltn = ltn.data("23-08-2024")  # doctest: +SKIP
    """
    df = anbima.tpf_data(date, "LTN")
    if df.is_empty():
        return df

    df_spread = di_spreads(date).select("MaturityDate", "DISpread")
    df = df.join(df_spread, on="MaturityDate", how="left").with_columns(
        pl.struct("IndicativeRate", "DIRate")
        .map_elements(
            lambda s: premium(s["IndicativeRate"], s["DIRate"]),
            return_dtype=pl.Float64,
        )
        .alias("Premium")
    )

    return df


def maturities(date: DateLike) -> pl.Series:
    """
    Busca os vencimentos disponíveis para a data de referência.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.Series: Série de datas de vencimento disponíveis.

    Examples:
        >>> from pyield import ltn
        >>> ltn.maturities("22-08-2024")
        shape: (13,)
        Series: 'MaturityDate' [date]
        [
            2024-10-01
            2025-01-01
            2025-04-01
            2025-07-01
            2025-10-01
            …
            2026-10-01
            2027-07-01
            2028-01-01
            2028-07-01
            2030-01-01
        ]
    """
    return data(date)["MaturityDate"]


def price(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula o preço (PU) da LTN pelas regras da ANBIMA.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto (YTM) do título.

    Returns:
        float: Preço (PU) da LTN conforme ANBIMA.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ltn
        >>> ltn.price("05-07-2024", "01-01-2030", 0.12145)
        535.279902
    """
    # Valida e normaliza entradas
    if any_is_empty(settlement, maturity, rate):
        return float("nan")
    # Calcula dias úteis entre liquidação e vencimento
    dias_uteis = bday.count(settlement, maturity)

    # Calcula anos úteis truncados conforme ANBIMA
    anos_truncados = utils.truncate(dias_uteis / 252, 14)

    fator_desconto = (1 + rate) ** anos_truncados

    # Trunca o preço em 6 casas conforme ANBIMA
    return utils.truncate(VALOR_FACE / fator_desconto, 6)


def rate(
    settlement: DateLike,
    maturity: DateLike,
    price_value: float,
) -> float:
    """
    Calcula a taxa implícita (YTM) de uma LTN a partir do preço (PU).

    Inverte algebricamente a fórmula de ``price()``:
    ``rate = (1000 / price) ^ (252 / du) - 1``

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        price_value (float): Preço unitário (PU) do título.

    Returns:
        float: Taxa implícita (YTM) em formato decimal. Retorna NaN em
            caso de erro.

    Examples:
        >>> from pyield import ltn
        >>> ltn.rate("05-07-2024", "01-01-2030", 535.279902)
        0.12145
        >>> ltn.rate("13-03-2026", "01-01-2027", 895.563913)
        0.148307
    """
    if any_is_empty(settlement, maturity, price_value):
        return float("nan")

    if price_value <= 0:
        return float("nan")

    dias_uteis = bday.count(settlement, maturity)
    anos_truncados = utils.truncate(dias_uteis / 252, 14)
    taxa = (VALOR_FACE / price_value) ** (1 / anos_truncados) - 1
    return round(taxa, 6)


def premium(ltn_rate: float, di_rate: float) -> float:
    """
    Calcula a rentabilidade da LTN sobre a taxa de DI Futuro.

    Args:
        ltn_rate (float): Taxa anualizada da LTN.
        di_rate (float): Taxa anualizada do DI Futuro.

    Returns:
        float: Rentabilidade da LTN sobre o DI.

    Examples:
        Reference date: 22-08-2024
        LTN rate for 01-01-2030: 0.118746
        DI (JAN30) Settlement rate: 0.11725
        >>> from pyield import ltn
        >>> ltn.premium(0.118746, 0.11725)
        1.0120718007994287
    """
    if any_is_empty(ltn_rate, di_rate):
        return float("nan")
    # Cálculo das taxas diárias
    taxa_diaria_ltn = (1 + ltn_rate) ** (1 / 252) - 1
    taxa_diaria_di = (1 + di_rate) ** (1 / 252) - 1

    # Retorno do cálculo do prêmio
    return taxa_diaria_ltn / taxa_diaria_di


def dv01(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da LTN em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto (YTM) do título.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ltn
        >>> ltn.dv01("26-03-2025", "01-01-2032", 0.150970)
        0.2269059999999854
    """
    if any_is_empty(settlement, maturity, rate):
        return float("nan")

    preco_1 = price(settlement, maturity, rate)
    preco_2 = price(settlement, maturity, rate + 0.0001)
    return preco_1 - preco_2


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
            Padrão False.

    Returns:
        pl.DataFrame: DataFrame com as colunas do spread.

    Output Columns:
        - BondType (String): Tipo do título.
        - MaturityDate (Date): Data de vencimento.
        - DISpread (Float64): Spread em decimal ou bps conforme parâmetro.

    Raises:
        ValueError: Se os dados de DI não possuem 'SettlementRate' ou estão vazios.

    Examples:
        >>> from pyield import ltn
        >>> ltn.di_spreads("30-05-2025", bps=True)
        shape: (13, 3)
        ┌──────────┬──────────────┬──────────┐
        │ BondType ┆ MaturityDate ┆ DISpread │
        │ ---      ┆ ---          ┆ ---      │
        │ str      ┆ date         ┆ f64      │
        ╞══════════╪══════════════╪══════════╡
        │ LTN      ┆ 2025-07-01   ┆ 4.39     │
        │ LTN      ┆ 2025-10-01   ┆ -9.0     │
        │ LTN      ┆ 2026-01-01   ┆ -4.88    │
        │ LTN      ┆ 2026-04-01   ┆ -4.45    │
        │ LTN      ┆ 2026-07-01   ┆ 0.81     │
        │ …        ┆ …            ┆ …        │
        │ LTN      ┆ 2028-01-01   ┆ 0.55     │
        │ LTN      ┆ 2028-07-01   ┆ 1.5      │
        │ LTN      ┆ 2029-01-01   ┆ 10.77    │
        │ LTN      ┆ 2030-01-01   ┆ 11.0     │
        │ LTN      ┆ 2032-01-01   ┆ 11.24    │
        └──────────┴──────────────┴──────────┘
    """
    return pre_di_spreads(date, bps=bps).filter(pl.col("BondType") == "LTN")


def forwards(date: DateLike) -> pl.DataFrame:
    """Calcula as taxas forward da LTN para uma data de referência.

    As taxas indicativas da LTN já são spot (zero-coupon) por construção, pois o
    título não paga cupons. Portanto o cálculo de forward é direto usando a
    estrutura de vencimentos e suas taxas.

    Args:
        date (DateLike): Data de referência das taxas indicativas.

    Returns:
        pl.DataFrame: DataFrame com as taxas forward.

    Output Columns:
        - MaturityDate (Date): Data de vencimento.
        - BDToMat (Int64): Dias úteis entre referência e vencimento.
        - IndicativeRate (Float64): Taxa spot (zero cupom).
        - ForwardRate (Float64): Taxa forward.

    Examples:
        >>> from pyield import ltn
        >>> ltn.forwards("17-10-2025")
        shape: (13, 4)
        ┌──────────────┬─────────┬────────────────┬─────────────┐
        │ MaturityDate ┆ BDToMat ┆ IndicativeRate ┆ ForwardRate │
        │ ---          ┆ ---     ┆ ---            ┆ ---         │
        │ date         ┆ i64     ┆ f64            ┆ f64         │
        ╞══════════════╪═════════╪════════════════╪═════════════╡
        │ 2026-01-01   ┆ 52      ┆ 0.148307       ┆ 0.148307    │
        │ 2026-04-01   ┆ 113     ┆ 0.147173       ┆ 0.146207    │
        │ 2026-07-01   ┆ 174     ┆ 0.145206       ┆ 0.141571    │
        │ 2026-10-01   ┆ 239     ┆ 0.142424       ┆ 0.13501     │
        │ 2027-04-01   ┆ 361     ┆ 0.138155       ┆ 0.129838    │
        │ …            ┆ …       ┆ …              ┆ …           │
        │ 2028-07-01   ┆ 676     ┆ 0.133411       ┆ 0.131654    │
        │ 2029-01-01   ┆ 800     ┆ 0.134254       ┆ 0.138861    │
        │ 2029-07-01   ┆ 924     ┆ 0.135264       ┆ 0.141802    │
        │ 2030-01-01   ┆ 1049    ┆ 0.135967       ┆ 0.141177    │
        │ 2032-01-01   ┆ 1553    ┆ 0.13883        ┆ 0.144812    │
        └──────────────┴─────────┴────────────────┴─────────────┘
    """
    if any_is_empty(date):
        return pl.DataFrame()
    df = data(date).select("MaturityDate", "BDToMat", "IndicativeRate")
    taxas_forward = fwd.forwards(bdays=df["BDToMat"], rates=df["IndicativeRate"])
    return df.with_columns(ForwardRate=taxas_forward).sort("MaturityDate")
