import polars as pl

import pyield._internal.converters as cv
from pyield import bday
from pyield._internal.types import DateLike, any_is_empty
from pyield.tn import utils


def data(date: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de LFT para a data de referência na ANBIMA.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de LFT.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "LFT").
        - codigo_selic (Int64): Código do título no SELIC.
        - data_base (Date): Data base/emissão do título.
        - data_vencimento (Date): Data de vencimento do título.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - prazo_medio (Float64): Prazo médio do título em anos.
        - pu (Float64): Preço unitário (PU).
        - taxa_compra (Float64): Taxa de compra (decimal).
        - taxa_venda (Float64): Taxa de venda (decimal).
        - taxa_indicativa (Float64): Taxa indicativa (decimal).
        - taxa_di (Float64): Taxa DI interpolada pelo método flat forward.
        - rentabilidade (Float64): Rentabilidade da LFT sobre o DI.

    Examples:
        >>> from pyield import lft
        >>> df_lft = lft.data("23-08-2024")  # doctest: +SKIP
    """
    df = utils.obter_tpf(date, "LFT")
    if df.is_empty():
        return df

    data_ref = cv.converter_datas(date)

    df = df.with_columns(
        dias_uteis=bday.count_expr("data_referencia", "data_vencimento"),
    )

    df = df.with_columns(
        prazo_medio=pl.col("dias_uteis") / 252,
    )
    df = utils.adicionar_taxa_di(df, data_ref)

    df = df.with_columns(
        rentabilidade=pl.struct("taxa_indicativa", "taxa_di").map_elements(
            lambda s: premium(s["taxa_indicativa"], s["taxa_di"]),
            return_dtype=pl.Float64,
        )
    )

    return df.select(
        "data_referencia",
        "titulo",
        "codigo_selic",
        "data_base",
        "data_vencimento",
        "dias_uteis",
        "prazo_medio",
        "pu",
        "taxa_compra",
        "taxa_venda",
        "taxa_indicativa",
        "taxa_di",
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
        >>> from pyield import lft
        >>> lft.maturities("22-08-2024")
        shape: (14,)
        Series: 'data_vencimento' [date]
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
    return data(date)["data_vencimento"]


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
    anos_truncados = utils.truncate(dias_uteis / 252, 14)

    fator_desconto = 1 / (1 + rate) ** anos_truncados

    return utils.truncate(100 * fator_desconto, 4)


def rate(
    settlement: DateLike,
    maturity: DateLike,
    vna: float,
    price_value: float,
) -> float:
    """
    Calcula a taxa implícita de uma LFT a partir do preço (PU).

    A função inverte numericamente a cadeia ``price(vna, quotation(...))``,
    encontrando a taxa que zera a diferença entre o preço calculado e o
    informado.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        vna (float): Valor nominal atualizado (VNA).
        price_value (float): Preço unitário (PU) do título.

    Returns:
        float: Taxa implícita em formato decimal. Retorna NaN em
            caso de erro.

    Examples:
        >>> from pyield import lft
        >>> lft.rate("24-07-2024", "01-09-2030", 15785.324502, 15621.867466)
        0.001717
        >>> lft.rate("24-07-2024", "01-03-2025", 15785.324502, 15774.132706)
        0.00116
    """
    if any_is_empty(settlement, maturity, vna, price_value):
        return float("nan")

    if price_value <= 0:
        return float("nan")

    def diferenca_preco(taxa: float) -> float:
        return price(vna, quotation(settlement, maturity, taxa)) - price_value

    taxa_encontrada = utils.encontrar_raiz(diferenca_preco)
    return round(taxa_encontrada, 6)


def premium(lft_rate: float, di_rate: float) -> float:
    """
    Calcula a rentabilidade da LFT sobre a taxa de DI Futuro.

    Args:
        lft_rate (float): Taxa anualizada da LFT sobre a Selic.
        di_rate (float): Taxa DI Futuro anualizada (interpolada para o mesmo
            vencimento da LFT).

    Returns:
        float: Rentabilidade da LFT sobre o DI.

    Examples:
        Calcula a rentabilidade de uma LFT em 28/04/2025:
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
    Calcula o preço (PU) da LFT pelas regras da Anbima.

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
    return utils.truncate(vna * quotation / 100, 6)
