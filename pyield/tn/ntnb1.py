from enum import Enum

import polars as pl
from dateutil.relativedelta import relativedelta

import pyield.converters as conversores
from pyield import bday
from pyield.tn import tools
from pyield.types import DateLike, any_is_empty

"""
Parâmetros globais para cálculos de NTN-B1.
Estes parâmetros definem o fluxo de amortização conforme o nome comercial.
"""


class CommercialName(Enum):
    """
    Enum do nome comercial usado para identificar o tipo de NTN-B1 (Renda+ ou Educa+).
    """

    RENDA_MAIS = "Renda+"
    EDUCA_MAIS = "Educa+"


# Mapeamento estático do número de meses (constante)
MAPA_PARAMETROS = {
    CommercialName.RENDA_MAIS: 240,
    CommercialName.EDUCA_MAIS: 60,
}


def _obter_parametros_titulo(
    nome_comercial: CommercialName,
) -> tuple[float, float, int]:
    """
    Retorna parâmetros de amortização conforme o nome comercial.

    Retorna: (pagamento_amortizacao, pagamento_amortizacao_final, numero_amortizacoes)
    """
    try:
        numero_amortizacoes = MAPA_PARAMETROS[nome_comercial]
    except KeyError:
        raise ValueError(f"Nome comercial inválido: {nome_comercial}")

    pagamento_amortizacao = 1 / numero_amortizacoes
    pagamento_amortizacao_final = 1 - (
        pagamento_amortizacao * (numero_amortizacoes - 1)
    )

    return pagamento_amortizacao, pagamento_amortizacao_final, numero_amortizacoes


def payment_dates(
    settlement: DateLike, maturity: DateLike, commercial_name: CommercialName
) -> pl.Series:
    """
    Gera todas as datas de amortização entre liquidação e vencimento.

    As datas são inclusivas. Os pagamentos ocorrem de 15/01 do ano de conversão
    até 15/12 do ano de vencimento.

    Args:
        settlement (DateLike): Data de liquidação (exclusiva).
        maturity (DateLike): Data de vencimento.
        commercial_name (CommercialName): Nome comercial (Renda+ ou Educa+).

    Returns:
        pl.Series: Série de datas de amortização no intervalo.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.payment_dates("10-05-2024", "15-12-2050", r_mais)
        shape: (240,)
        Series: 'payment_dates' [date]
        [
            2031-01-15
            2031-02-15
            2031-03-15
            2031-04-15
            2031-05-15
            …
            2050-08-15
            2050-09-15
            2050-10-15
            2050-11-15
            2050-12-15
        ]
    """
    if any_is_empty(settlement, maturity, commercial_name):
        return pl.Series("payment_dates", dtype=pl.Date)

    # Valida e normaliza datas
    liquidacao = conversores.converter_datas(settlement)
    vencimento = conversores.converter_datas(maturity)

    if vencimento <= liquidacao:
        raise ValueError("A data de vencimento deve ser posterior à liquidação.")

    vencimento = vencimento.replace(day=15)

    # Parâmetros do título
    _, _, numero_amortizacoes = _obter_parametros_titulo(commercial_name)

    datas_amortizacao = [
        vencimento - relativedelta(months=i) for i in range(numero_amortizacoes)
    ]

    if len(datas_amortizacao) == 0:
        raise ValueError("Nenhuma data de amortização após a liquidação.")

    datas_pagamento = pl.Series(name="payment_dates", values=datas_amortizacao).cast(
        pl.Date
    )

    return datas_pagamento.filter(datas_pagamento > liquidacao).sort()


def cash_flows(
    settlement: DateLike, maturity: DateLike, commercial_name: CommercialName
) -> pl.DataFrame:
    """
    Gera os fluxos de caixa da NTN-B1 entre liquidação e vencimento.

    Args:
        settlement (DateLike): Data de liquidação (exclusiva).
        maturity (DateLike): Data de vencimento.
        commercial_name (CommercialName): Nome comercial (Renda+ ou Educa+).

    Returns:
        pl.DataFrame: DataFrame com as colunas de fluxo.

    Output Columns:
        * PaymentDate (Date): Data de pagamento do fluxo.
        * CashFlow (Float64): Valor do fluxo.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.cash_flows("10-05-2024", "15-12-2060", r_mais)
        shape: (240, 2)
        ┌─────────────┬──────────┐
        │ PaymentDate ┆ CashFlow │
        │ ---         ┆ ---      │
        │ date        ┆ f64      │
        ╞═════════════╪══════════╡
        │ 2041-01-15  ┆ 0.004167 │
        │ 2041-02-15  ┆ 0.004167 │
        │ 2041-03-15  ┆ 0.004167 │
        │ 2041-04-15  ┆ 0.004167 │
        │ 2041-05-15  ┆ 0.004167 │
        │ …           ┆ …        │
        │ 2060-08-15  ┆ 0.004167 │
        │ 2060-09-15  ┆ 0.004167 │
        │ 2060-10-15  ┆ 0.004167 │
        │ 2060-11-15  ┆ 0.004167 │
        │ 2060-12-15  ┆ 0.004167 │
        └─────────────┴──────────┘

    """
    if any_is_empty(settlement, maturity, commercial_name):
        return pl.DataFrame({"PaymentDate": [], "CashFlow": []})

    # Valida e normaliza datas
    liquidacao = conversores.converter_datas(settlement)
    vencimento = conversores.converter_datas(maturity)

    # Obtém as datas de amortização
    datas_pagamento = payment_dates(liquidacao, vencimento, commercial_name)
    df = pl.DataFrame({"PaymentDate": datas_pagamento})

    # Parâmetros do título
    pagamento_amort, pagamento_amort_final, _ = _obter_parametros_titulo(
        commercial_name
    )

    # Define o fluxo final no vencimento e os demais como amortizações
    df = df.with_columns(
        pl.when(pl.col("PaymentDate") == vencimento)
        .then(pagamento_amort_final)
        .otherwise(pagamento_amort)
        .alias("CashFlow")
    )

    # Retorna o DataFrame com datas e fluxos
    return df


def quotation(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    commercial_name: CommercialName,
) -> float:
    """
    Calcula a cotação da NTN-B1 em base 100 pelas regras da ANBIMA.

    Args:
        settlement (DateLike): Data de liquidação da operação.
        maturity (DateLike): Data de vencimento da NTN-B1.
        rate (float): Taxa de desconto (YTM) usada no valor presente.
        commercial_name (CommercialName): Nome comercial (Renda+ ou Educa+).

    Returns:
        float: Cotação da NTN-B1 truncada em 6 casas decimais.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.quotation("18-06-2025", "15-12-2084", 0.07010, r_mais)
        0.038332
    """
    if any_is_empty(settlement, maturity, rate, commercial_name):
        return float("nan")

    df = cash_flows(settlement, maturity, commercial_name)
    datas_fluxo = df["PaymentDate"]
    valores_fluxo = df["CashFlow"]

    # Calcula dias úteis entre liquidação e fluxos
    dias_uteis = bday.count(settlement, datas_fluxo)

    # Calcula anos úteis truncados conforme ANBIMA
    anos_uteis = tools.truncate(dias_uteis / 252, 14)

    fator_desconto = (1 + rate) ** anos_uteis

    # Calcula o valor presente de cada fluxo (DCF) com arredondamento ANBIMA
    valor_presente_fluxos = (valores_fluxo / fator_desconto).round(10)

    # Retorna a cotação (soma do DCF) com truncamento ANBIMA
    return tools.truncate(valor_presente_fluxos.sum(), 6)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calcula o preço da NTN-B1 pelas regras do Tesouro Nacional.

    Args:
        vna (float): Valor nominal atualizado (VNA).
        quotation (float): Cotação da NTN-B1 em base 100.

    Returns:
        float: Preço da NTN-B1 truncado em 6 casas decimais.

    References:
         - SEI Proccess 17944.005214/2024-09

    Examples:
        >>> from pyield import ntnb1
        >>> ntnb1.price(4299.160173, 99.3651 / 100)
        4271.864805
        >>> ntnb1.price(4315.498383, 100.6409 / 100)
        4343.156412
    """
    if any_is_empty(vna, quotation):
        return float("nan")
    return tools.truncate(vna * quotation, 6)


def duration(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    commercial_name: CommercialName,
) -> float:
    """
    Calcula a Macaulay duration da NTN-B1 em anos úteis.

    Args:
        settlement (DateLike): Data de liquidação da operação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto usada no cálculo.
        commercial_name (CommercialName): Nome comercial (Renda+ ou Educa+).

    Returns:
        float: Macaulay duration em anos úteis.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.duration("23-06-2025", "15-12-2084", 0.0686, r_mais)
        47.10493458167134
    """
    # Retorna NaN se houver entradas nulas
    if any_is_empty(settlement, maturity, rate, commercial_name):
        return float("nan")

    df = cash_flows(settlement, maturity, commercial_name)
    anos_uteis = bday.count(settlement, df["PaymentDate"]) / 252
    dcf = df["CashFlow"] / (1 + rate) ** anos_uteis
    duracao = (dcf * anos_uteis).sum() / dcf.sum()

    # Trunca a duração para 14 casas para reprodutibilidade
    return tools.truncate(duracao, 14)


def dv01(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    vna: float,
    commercial_name: CommercialName = CommercialName.RENDA_MAIS,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B1 em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto (YTM) da NTN-B1.
        vna (float): Valor nominal atualizado (VNA).
        commercial_name (CommercialName): Nome comercial (Renda+ ou Educa+).

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.CommercialName.RENDA_MAIS
        >>> ntnb1.dv01("23-06-2025", "15-12-2084", 0.0686, 4299.160173, r_mais)
        0.7738490000000127
    """
    if any_is_empty(settlement, maturity, rate, vna, commercial_name):
        return float("nan")

    quotation1 = quotation(settlement, maturity, rate, commercial_name)
    quotation2 = quotation(settlement, maturity, rate + 0.0001, commercial_name)
    price1 = price(vna, quotation1)
    price2 = price(vna, quotation2)
    return price1 - price2
