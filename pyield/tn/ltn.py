import polars as pl

import pyield._internal.converters as cv
from pyield import bday, fwd
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
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "LTN").
        - codigo_selic (Int64): Código do título no SELIC.
        - data_base (Date): Data base/emissão do título.
        - data_vencimento (Date): Data de vencimento do título.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - duration (Float64): Macaulay Duration do título (anos).
        - prazo_medio (Float64): Prazo médio do título (anos).
        - dv01 (Float64): Variação no preço para 1bp de taxa.
        - dv01_usd (Float64): DV01 convertido para USD pela PTAX do dia.
        - pu (Float64): Preço unitário (PU).
        - taxa_compra (Float64): Taxa de compra (decimal).
        - taxa_venda (Float64): Taxa de venda (decimal).
        - taxa_indicativa (Float64): Taxa indicativa (decimal).
        - taxa_di (Float64): Taxa DI interpolada pelo método flat forward.
        - spread_di (Float64): Spread sobre o DI (também conhecido como
            prêmio).
        - rentabilidade (Float64): Rentabilidade diária da LTN sobre o DI.

    Examples:
        >>> from pyield import ltn
        >>> df_ltn = ltn.data("23-08-2024")  # doctest: +SKIP
    """
    df = utils.obter_tpf(date, "LTN")
    if df.is_empty():
        return df

    data_ref = cv.converter_datas(date)

    df = df.with_columns(
        dias_uteis=bday.count_expr("data_referencia", "data_vencimento"),
    )

    df = df.with_columns(
        duration=pl.col("dias_uteis") / 252,
    ).with_columns(prazo_medio=pl.col("duration"))
    df = utils.adicionar_dv01(df, data_ref)
    df = utils.adicionar_taxa_di(df, data_ref)

    df = df.with_columns(
        spread_di=pl.col("taxa_indicativa") - pl.col("taxa_di"),
        rentabilidade=pl.struct("taxa_indicativa", "taxa_di").map_elements(
            lambda s: premium(s["taxa_indicativa"], s["taxa_di"]),
            return_dtype=pl.Float64,
        ),
    )

    return df.select(
        "data_referencia",
        "titulo",
        "codigo_selic",
        "data_base",
        "data_vencimento",
        "dias_uteis",
        "duration",
        "prazo_medio",
        "dv01",
        "dv01_usd",
        "pu",
        "taxa_compra",
        "taxa_venda",
        "taxa_indicativa",
        "taxa_di",
        "spread_di",
        "rentabilidade",
    )


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
        Series: 'data_vencimento' [date]
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
    return data(date)["data_vencimento"]


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

    # Retorno do cálculo da rentabilidade
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
        spread_di = taxa_indicativa - taxa de ajuste do DI

    Quando ``bps=False`` a coluna retorna essa diferença em formato decimal
    (ex: 0.000439 ≈ 4.39 bps). Quando ``bps=True`` o valor é automaticamente
    multiplicado por 10_000 e exibido diretamente em basis points.

    Args:
        date (DateLike): Data de referência para buscar as taxas.
        bps (bool): Se True, retorna spread_di já convertido em basis points.
            Padrão False.

    Returns:
        pl.DataFrame: DataFrame com as colunas do spread.

    Output Columns:
        - titulo (String): Tipo do título.
        - data_vencimento (Date): Data de vencimento.
        - spread_di (Float64): Spread em decimal ou bps conforme parâmetro
            (também conhecido como prêmio).

    Raises:
        ValueError: Se os dados de DI não possuem 'SettlementRate' ou estão vazios.

    Examples:
        >>> from pyield import ltn
        >>> ltn.di_spreads("30-05-2025", bps=True)
        shape: (13, 3)
        ┌────────┬─────────────────┬───────────┐
        │ titulo ┆ data_vencimento ┆ spread_di │
        │ ---    ┆ ---             ┆ ---       │
        │ str    ┆ date            ┆ f64       │
        ╞════════╪═════════════════╪═══════════╡
        │ LTN    ┆ 2025-07-01      ┆ 4.39      │
        │ LTN    ┆ 2025-10-01      ┆ -9.0      │
        │ LTN    ┆ 2026-01-01      ┆ -4.88     │
        │ LTN    ┆ 2026-04-01      ┆ -4.45     │
        │ LTN    ┆ 2026-07-01      ┆ 0.81      │
        │ …      ┆ …               ┆ …         │
        │ LTN    ┆ 2028-01-01      ┆ 0.55      │
        │ LTN    ┆ 2028-07-01      ┆ 1.5       │
        │ LTN    ┆ 2029-01-01      ┆ 10.77     │
        │ LTN    ┆ 2030-01-01      ┆ 11.0      │
        │ LTN    ┆ 2032-01-01      ┆ 11.24     │
        └────────┴─────────────────┴───────────┘
    """
    return pre_di_spreads(date, bps=bps).filter(pl.col("titulo") == "LTN")


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
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - taxa_indicativa (Float64): Taxa spot (zero cupom).
        - taxa_forward (Float64): Taxa forward.

    Examples:
        >>> from pyield import ltn
        >>> ltn.forwards("17-10-2025")
        shape: (13, 4)
        ┌─────────────────┬────────────┬─────────────────┬──────────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_indicativa ┆ taxa_forward │
        │ ---             ┆ ---        ┆ ---             ┆ ---          │
        │ date            ┆ i64        ┆ f64             ┆ f64          │
        ╞═════════════════╪════════════╪═════════════════╪══════════════╡
        │ 2026-01-01      ┆ 52         ┆ 0.148307        ┆ 0.148307     │
        │ 2026-04-01      ┆ 113        ┆ 0.147173        ┆ 0.146207     │
        │ 2026-07-01      ┆ 174        ┆ 0.145206        ┆ 0.141571     │
        │ 2026-10-01      ┆ 239        ┆ 0.142424        ┆ 0.13501      │
        │ 2027-04-01      ┆ 361        ┆ 0.138155        ┆ 0.129838     │
        │ …               ┆ …          ┆ …               ┆ …            │
        │ 2028-07-01      ┆ 676        ┆ 0.133411        ┆ 0.131654     │
        │ 2029-01-01      ┆ 800        ┆ 0.134254        ┆ 0.138861     │
        │ 2029-07-01      ┆ 924        ┆ 0.135264        ┆ 0.141802     │
        │ 2030-01-01      ┆ 1049       ┆ 0.135967        ┆ 0.141177     │
        │ 2032-01-01      ┆ 1553       ┆ 0.13883         ┆ 0.144812     │
        └─────────────────┴────────────┴─────────────────┴──────────────┘
    """
    if any_is_empty(date):
        return pl.DataFrame()
    df = data(date).select("data_vencimento", "dias_uteis", "taxa_indicativa")
    taxas_forward = fwd.forwards(bdays=df["dias_uteis"], rates=df["taxa_indicativa"])
    return df.with_columns(taxa_forward=taxas_forward).sort("data_vencimento")
