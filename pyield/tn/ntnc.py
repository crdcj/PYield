import datetime as dt

import polars as pl
from dateutil.relativedelta import relativedelta

import pyield._internal.converters as conversores
import pyield.tn.tools as ferramentas
from pyield import anbima, bday
from pyield._internal.types import DateLike, any_is_empty

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
        * BondType (String): Tipo do título (ex.: "NTN-C").
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

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.data("23-08-2024")
        shape: (1, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬──────────┬──────────┬────────────────┬─────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate  ┆ AskRate  ┆ IndicativeRate ┆ DIRate  │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---      ┆ ---      ┆ ---            ┆ ---     │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64      ┆ f64      ┆ f64            ┆ f64     │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪══════════╪══════════╪════════════════╪═════════╡
        │ 2024-08-23    ┆ NTN-C    ┆ 770100    ┆ 2000-07-01    ┆ … ┆ 0.061591 ┆ 0.057587 ┆ 0.059617       ┆ 0.11575 │
        └───────────────┴──────────┴───────────┴───────────────┴───┴──────────┴──────────┴────────────────┴─────────┘
    """  # noqa: E501
    return anbima.tpf_data(date, "NTN-C")


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
        Series: '' [date]
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
        return pl.Series(dtype=pl.Date)

    # Valida e normaliza datas
    liquidacao = conversores.converter_datas(settlement)
    vencimento = conversores.converter_datas(maturity)

    # Check if maturity date is after the start date
    if vencimento < liquidacao:
        return pl.Series(dtype=pl.Date)

    # Initialize loop variables
    data_cupom = vencimento
    datas_cupons = []

    # Iterate backwards from the maturity date to the settlement date
    while data_cupom > liquidacao:
        datas_cupons.append(data_cupom)
        # Retrocede 6 meses
        data_cupom -= relativedelta(months=6)

    return pl.Series(datas_cupons).sort()


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
        * PaymentDate (Date): Data de pagamento do fluxo.
        * CashFlow (Float64): Valor do fluxo.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.cash_flows("21-03-2025", "01-01-2031")
        shape: (12, 2)
        ┌─────────────┬────────────┐
        │ PaymentDate ┆ CashFlow   │
        │ ---         ┆ ---        │
        │ date        ┆ f64        │
        ╞═════════════╪════════════╡
        │ 2025-07-01  ┆ 5.830052   │
        │ 2026-01-01  ┆ 5.830052   │
        │ 2026-07-01  ┆ 5.830052   │
        │ 2027-01-01  ┆ 5.830052   │
        │ 2027-07-01  ┆ 5.830052   │
        │ …           ┆ …          │
        │ 2029-01-01  ┆ 5.830052   │
        │ 2029-07-01  ┆ 5.830052   │
        │ 2030-01-01  ┆ 5.830052   │
        │ 2030-07-01  ┆ 5.830052   │
        │ 2031-01-01  ┆ 105.830052 │
        └─────────────┴────────────┘
    """
    if any_is_empty(settlement, maturity):
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    # Valida e normaliza datas
    liquidacao = conversores.converter_datas(settlement)
    vencimento = conversores.converter_datas(maturity)

    # Obtém as datas de cupom entre liquidação e vencimento
    datas_pagamento = payment_dates(liquidacao, vencimento)

    # Retorna DataFrame vazio se não houver pagamentos (liquidação >= vencimento)
    if datas_pagamento.is_empty():
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    # Obtém os valores corretos de cupom e final
    valor_cupom = _obter_valor_cupom(vencimento)
    valor_final = _obter_valor_final(vencimento)

    # Build dataframe and assign cash flows using Polars expressions
    df = pl.DataFrame({"PaymentDate": datas_pagamento}).with_columns(
        pl.when(pl.col("PaymentDate") == vencimento)
        .then(valor_final)
        .otherwise(valor_cupom)
        .alias("CashFlow")
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

    df = cash_flows(settlement, maturity)
    if df.is_empty():
        return float("nan")

    datas_fluxo = df["PaymentDate"]
    valores_fluxo = df["CashFlow"]

    # Calcula dias úteis entre liquidação e fluxos
    dias_uteis = bday.count(settlement, datas_fluxo)

    # Calcula anos úteis truncados conforme ANBIMA
    anos_uteis = ferramentas.truncate(dias_uteis / 252, 14)

    fator_desconto = (1 + rate) ** anos_uteis

    # Calcula o valor presente de cada fluxo (DCF) com arredondamento ANBIMA
    valor_presente_fluxos = (valores_fluxo / fator_desconto).round(10)

    # Return the quotation (the dcf sum) truncated as per Anbima rules
    return ferramentas.truncate(valor_presente_fluxos.sum(), 4)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calcula o preço da NTN-C pelas regras da ANBIMA.

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
    return ferramentas.truncate(vna * quotation / 100, 6)


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

    df = cash_flows(settlement, maturity)
    if df.is_empty():
        return float("nan")

    anos_uteis = bday.count(settlement, df["PaymentDate"]) / 252
    dcf = df["CashFlow"] / (1 + rate) ** anos_uteis
    duracao = (dcf * anos_uteis).sum() / dcf.sum()
    # Truncar para 14 casas decimais para repetibilidade dos resultados
    return ferramentas.truncate(duracao, 14)
