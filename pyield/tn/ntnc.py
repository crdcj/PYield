import datetime as dt

import polars as pl

import pyield._internal.converters as conversores
from pyield import anbima, bday
from pyield._internal.types import DateLike, any_is_empty
from pyield.tn import utils

"""
Constantes calculadas conforme regras da ANBIMA e em base 100.
Válido para NTN-C com vencimento 01-01-2031:
PRINCIPAL = 100
TAXA_CUPOM = (0.12 + 1) ** 0.5 - 1  # 12% a.a. com capitalização semestral
VALOR_CUPOM_2031 = round(100 * TAXA_CUPOM, 6) -> 5.830052
VALOR_FINAL_2031 = principal + último cupom = 100 + 5.830052

Para as demais NTN-C:
TAXA_CUPOM = (0.06 + 1) ** 0.5 - 1  # 6% a.a. com capitalização semestral
VALOR_CUPOM = round(100 * TAXA_CUPOM, 6) -> 2.956301
VALOR_FINAL = principal + último cupom = 100 + 2.956301
"""
VALOR_CUPOM_2031 = 5.830052
VALOR_FINAL_2031 = 105.830052

VALOR_CUPOM = 2.956301
VALOR_FINAL = 102.956301


def _obter_valor_cupom(vencimento: dt.date) -> float:
    if vencimento.year == 2031:  # noqa
        return VALOR_CUPOM_2031
    return VALOR_CUPOM


def _obter_valor_final(vencimento: dt.date) -> float:
    if vencimento.year == 2031:  # noqa
        return VALOR_FINAL_2031
    return VALOR_FINAL


def data(date: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de NTN-C para a data de referência.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de NTN-C.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "NTN-C").
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
        - taxa_di (Float64): Taxa DI interpolada (flat forward).

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.data("23-08-2024")  # doctest: +SKIP
    """
    df = utils.renomear_colunas_tpf(anbima.tpf(date, "NTN-C"))
    if df.is_empty():
        return df

    data_ref = conversores.converter_datas(date)

    # Adiciona dias_uteis (dado derivado, não vem da ANBIMA)
    df = df.with_columns(
        dias_uteis=bday.count_expr("data_referencia", "data_vencimento"),
    )

    # Adiciona duration, prazo_medio, dv01, dv01_usd e taxa_di
    df = utils.adicionar_duration(df, duration)
    df = utils.adicionar_dv01(df, data_ref)
    df = utils.adicionar_taxa_di(df, data_ref)

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
    )


def payment_dates(
    settlement: DateLike,
    maturity: DateLike,
) -> pl.Series:
    """
    Gera todas as datas de cupom entre liquidação e vencimento (inclusivas).
    A NTN-C é definida pela data de vencimento.

    Args:
        settlement (DateLike): Data de liquidação (exclusiva).
        maturity (DateLike): Data de vencimento.

    Returns:
        pl.Series: Série de datas de cupom no intervalo. Retorna série vazia se
            vencimento for menor que a liquidação.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.payment_dates("21-03-2025", "01-01-2031")
        shape: (12,)
        Series: 'datas_pagamento' [date]
        [
            2025-07-01
            2026-01-01
            2026-07-01
            2027-01-01
            2027-07-01
            …
            2029-01-01
            2029-07-01
            2030-01-01
            2030-07-01
            2031-01-01
        ]
    """
    if any_is_empty(settlement, maturity):
        return pl.Series(name="datas_pagamento", dtype=pl.Date)

    # Valida e normaliza datas
    liquidacao = conversores.converter_datas(settlement)
    vencimento = conversores.converter_datas(maturity)

    # Check if maturity date is after the start date
    if vencimento < liquidacao:
        return pl.Series(name="datas_pagamento", dtype=pl.Date)

    # Initialize loop variables
    data_cupom = vencimento
    datas_cupons = []

    # Iterate backwards from the maturity date to the settlement date
    while data_cupom > liquidacao:
        datas_cupons.append(data_cupom)
        # Retrocede 6 meses
        data_cupom = utils.subtrair_meses(data_cupom, 6)

    return pl.Series(name="datas_pagamento", values=datas_cupons).sort()


def cash_flows(
    settlement: DateLike,
    maturity: DateLike,
) -> pl.DataFrame:
    """
    Gera os fluxos de caixa da NTN-C entre liquidação e vencimento.

    Args:
        settlement (DateLike): Data de liquidação (exclusiva).
        maturity (DateLike): Data de vencimento.

    Returns:
        pl.DataFrame: DataFrame com as colunas de fluxo.

    Output Columns:
        - data_pagamento (Date): Data de pagamento.
        - valor_pagamento (Float64): Valor do pagamento.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.cash_flows("21-03-2025", "01-01-2031")
        shape: (12, 2)
        ┌────────────────┬─────────────────┐
        │ data_pagamento ┆ valor_pagamento │
        │ ---            ┆ ---             │
        │ date           ┆ f64             │
        ╞════════════════╪═════════════════╡
        │ 2025-07-01     ┆ 5.830052        │
        │ 2026-01-01     ┆ 5.830052        │
        │ 2026-07-01     ┆ 5.830052        │
        │ 2027-01-01     ┆ 5.830052        │
        │ 2027-07-01     ┆ 5.830052        │
        │ …              ┆ …               │
        │ 2029-01-01     ┆ 5.830052        │
        │ 2029-07-01     ┆ 5.830052        │
        │ 2030-01-01     ┆ 5.830052        │
        │ 2030-07-01     ┆ 5.830052        │
        │ 2031-01-01     ┆ 105.830052      │
        └────────────────┴─────────────────┘
    """
    if any_is_empty(settlement, maturity):
        return pl.DataFrame(
            schema={"data_pagamento": pl.Date, "valor_pagamento": pl.Float64}
        )

    # Valida e normaliza datas
    liquidacao = conversores.converter_datas(settlement)
    vencimento = conversores.converter_datas(maturity)

    # Obtém as datas de cupom entre liquidação e vencimento
    datas_pagamento = payment_dates(liquidacao, vencimento)

    # Retorna DataFrame vazio se não houver pagamentos (liquidação >= vencimento)
    if datas_pagamento.is_empty():
        return pl.DataFrame(
            schema={"data_pagamento": pl.Date, "valor_pagamento": pl.Float64}
        )

    # Obtém os valores corretos de cupom e final
    valor_cupom = _obter_valor_cupom(vencimento)
    valor_final = _obter_valor_final(vencimento)

    # Build dataframe and assign cash flows using Polars expressions
    df = pl.DataFrame({"data_pagamento": datas_pagamento}).with_columns(
        pl.when(pl.col("data_pagamento") == vencimento)
        .then(valor_final)
        .otherwise(valor_cupom)
        .alias("valor_pagamento")
    )
    return df


def quotation(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula a cotação da NTN-C em base 100 pelas regras da ANBIMA.

    Args:
        settlement (DateLike): Data de liquidação da operação.
        maturity (DateLike): Data de vencimento da NTN-C.
        rate (float): Taxa de desconto (YTM) usada no valor presente.

    Returns:
        float: Cotação da NTN-C truncada em 4 casas decimais.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - O cupom semestral é 2,956301, equivalente a 6% a.a. com capitalização
          semestral e arredondamento para 6 casas, conforme ANBIMA.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.quotation("21-03-2025", "01-01-2031", 0.067626)
        126.4958
    """
    if any_is_empty(settlement, maturity, rate):
        return float("nan")

    df_fluxos = cash_flows(settlement, maturity)
    if df_fluxos.is_empty():
        return float("nan")

    valores_fluxo = df_fluxos["valor_pagamento"]
    dias_uteis = bday.count(settlement, df_fluxos["data_pagamento"])
    anos_uteis = utils.truncate(dias_uteis / 252, 14)
    fatores_desconto = (1 + rate) ** anos_uteis
    # Calcula o valor presente de cada fluxo com arredondamento ANBIMA
    vp = (valores_fluxo / fatores_desconto).round(10)
    # Retorna a cotação (soma dos valores presentes) com truncamento ANBIMA
    return utils.truncate(vp.sum(), 4)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calcula o preço (PU) da NTN-C pelas regras da ANBIMA.

    price = VNA * quotation / 100

    Args:
        vna (float): Valor nominal atualizado (VNA).
        quotation (float): Cotação da NTN-C em base 100.

    Returns:
        float: Preço da NTN-C truncado em 6 casas decimais.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.price(6598.913723, 126.4958)
        8347.348705
    """
    if any_is_empty(vna, quotation):
        return float("nan")
    return utils.truncate(vna * quotation / 100, 6)


def rate(
    settlement: DateLike,
    maturity: DateLike,
    vna: float,
    price_value: float,
) -> float:
    """
    Calcula a taxa implícita (YTM) de uma NTN-C a partir do preço (PU).

    A função inverte numericamente a cadeia ``price(vna, quotation(...))``,
    encontrando a taxa que zera a diferença entre o preço calculado e o
    informado.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        vna (float): Valor nominal atualizado (VNA).
        price_value (float): Preço unitário (PU) do título.

    Returns:
        float: Taxa implícita (YTM) em formato decimal. Retorna NaN em
            caso de erro.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.rate("21-03-2025", "01-01-2031", 6598.913723, 8347.348705)
        0.067626
    """
    if any_is_empty(settlement, maturity, vna, price_value):
        return float("nan")

    if price_value <= 0:
        return float("nan")

    def diferenca_preco(taxa: float) -> float:
        cotacao = quotation(settlement, maturity, taxa)
        return price(vna, cotacao) - price_value

    taxa_encontrada = utils.encontrar_raiz(diferenca_preco)
    return round(taxa_encontrada, 6)


def duration(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula a Macaulay duration da NTN-C em anos úteis.

    Args:
        settlement (DateLike): Data de liquidação da operação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto usada no cálculo.

    Returns:
        float: Macaulay duration em anos úteis.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.duration("21-03-2025", "01-01-2031", 0.067626)
        4.405363320448
    """
    if any_is_empty(settlement, maturity, rate):
        return float("nan")

    df_fluxos = cash_flows(settlement, maturity)
    if df_fluxos.is_empty():
        return float("nan")

    anos_uteis = bday.count(settlement, df_fluxos["data_pagamento"]) / 252
    vp = df_fluxos["valor_pagamento"] / (1 + rate) ** anos_uteis
    duracao = float((vp * anos_uteis).sum()) / float(vp.sum())
    # Truncar para 14 casas decimais para repetibilidade dos resultados
    return utils.truncate(duracao, 14)
