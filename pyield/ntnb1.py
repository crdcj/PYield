from enum import Enum

import polars as pl

import pyield._internal.converters as conversores
from pyield import du
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf import utils

"""
Parâmetros globais para cálculos de NTN-B1.
Estes parâmetros definem o fluxo de amortização conforme o nome comercial.
"""


class NomeComercial(Enum):
    """
    Enum do nome comercial usado para identificar o tipo de NTN-B1 (Renda+ ou Educa+).
    """

    RENDA_MAIS = "Renda+"
    EDUCA_MAIS = "Educa+"


# Mapeamento estático do número de meses (constante)
MAPA_PARAMETROS = {
    NomeComercial.RENDA_MAIS: 240,
    NomeComercial.EDUCA_MAIS: 60,
}


def _obter_parametros_titulo(
    nome_comercial: NomeComercial,
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


def datas_pagamento(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    nome_comercial: NomeComercial,
) -> pl.Series:
    """
    Gera todas as datas de amortização entre liquidação e vencimento.

    As datas são inclusivas. Os pagamentos ocorrem de 15/01 do ano de conversão
    até 15/12 do ano de vencimento.

    Args:
        data_liquidacao (DateLike): Data de liquidação (exclusiva).
        data_vencimento (DateLike): Data de vencimento.
        nome_comercial (NomeComercial): Nome comercial (Renda+ ou Educa+).

    Returns:
        pl.Series: Série de datas de amortização no intervalo.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
        >>> ntnb1.datas_pagamento("10-05-2024", "15-12-2050", r_mais)
        shape: (240,)
        Series: 'datas_pagamento' [date]
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
    if any_is_empty(data_liquidacao, data_vencimento, nome_comercial):
        return pl.Series("datas_pagamento", dtype=pl.Date)

    # Valida e normaliza datas
    liquidacao = conversores.converter_datas(data_liquidacao)
    vencimento = conversores.converter_datas(data_vencimento)

    if vencimento <= liquidacao:
        raise ValueError("A data de vencimento deve ser posterior à liquidação.")

    vencimento = vencimento.replace(day=15)

    # Parâmetros do título
    _, _, numero_amortizacoes = _obter_parametros_titulo(nome_comercial)

    datas_amortizacao = [
        utils.subtrair_meses(vencimento, i) for i in range(numero_amortizacoes)
    ]

    if len(datas_amortizacao) == 0:
        raise ValueError("Nenhuma data de amortização após a liquidação.")

    datas_pagamento = pl.Series(name="datas_pagamento", values=datas_amortizacao).cast(
        pl.Date
    )

    return datas_pagamento.filter(datas_pagamento > liquidacao).sort()


def fluxos_caixa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    nome_comercial: NomeComercial,
) -> pl.DataFrame:
    """
    Gera os fluxos de caixa da NTN-B1 entre liquidação e vencimento.

    Args:
        data_liquidacao (DateLike): Data de liquidação (exclusiva).
        data_vencimento (DateLike): Data de vencimento.
        nome_comercial (NomeComercial): Nome comercial (Renda+ ou Educa+).

    Returns:
        pl.DataFrame: DataFrame com as colunas de fluxo.

    Output Columns:
        - data_pagamento (Date): Data de pagamento.
        - valor_pagamento (Float64): Valor do pagamento.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
        >>> ntnb1.fluxos_caixa("10-05-2024", "15-12-2060", r_mais)
        shape: (240, 2)
        ┌────────────────┬─────────────────┐
        │ data_pagamento ┆ valor_pagamento │
        │ ---            ┆ ---             │
        │ date           ┆ f64             │
        ╞════════════════╪═════════════════╡
        │ 2041-01-15     ┆ 0.004167        │
        │ 2041-02-15     ┆ 0.004167        │
        │ 2041-03-15     ┆ 0.004167        │
        │ 2041-04-15     ┆ 0.004167        │
        │ 2041-05-15     ┆ 0.004167        │
        │ …              ┆ …               │
        │ 2060-08-15     ┆ 0.004167        │
        │ 2060-09-15     ┆ 0.004167        │
        │ 2060-10-15     ┆ 0.004167        │
        │ 2060-11-15     ┆ 0.004167        │
        │ 2060-12-15     ┆ 0.004167        │
        └────────────────┴─────────────────┘

    """
    if any_is_empty(data_liquidacao, data_vencimento, nome_comercial):
        return pl.DataFrame({"data_pagamento": [], "valor_pagamento": []})

    # Valida e normaliza datas
    liquidacao = conversores.converter_datas(data_liquidacao)
    vencimento = conversores.converter_datas(data_vencimento)

    # Obtém as datas de amortização
    serie_datas_pagamento = datas_pagamento(liquidacao, vencimento, nome_comercial)
    df = pl.DataFrame({"data_pagamento": serie_datas_pagamento})

    # Parâmetros do título
    pagamento_amort, pagamento_amort_final, _ = _obter_parametros_titulo(nome_comercial)

    # Define o fluxo final no vencimento e os demais como amortizações
    df = df.with_columns(
        pl.when(pl.col("data_pagamento") == vencimento)
        .then(pagamento_amort_final)
        .otherwise(pagamento_amort)
        .alias("valor_pagamento")
    )

    # Retorna o DataFrame com datas e fluxos
    return df


def cotacao(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
    nome_comercial: NomeComercial,
) -> float:
    """
    Calcula a cotação da NTN-B1 em base 100 pelas regras da ANBIMA.

    Args:
        data_liquidacao (DateLike): Data de liquidação da operação.
        data_vencimento (DateLike): Data de vencimento da NTN-B1.
        taxa (float): Taxa de desconto (YTM) usada no valor presente.
        nome_comercial (NomeComercial): Nome comercial (Renda+ ou Educa+).

    Returns:
        float: Cotação da NTN-B1 truncada em 6 casas decimais.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
        >>> ntnb1.cotacao("18-06-2025", "15-12-2084", 0.07010, r_mais)
        0.038332
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa, nome_comercial):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento, nome_comercial)
    valores_fluxo = df_fluxos["valor_pagamento"]
    dias_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"])
    anos_uteis = utils.truncar(dias_uteis / 252, 14)
    fatores_desconto = (1 + taxa) ** anos_uteis
    # Calcula o valor presente de cada fluxo com arredondamento ANBIMA
    vp = (valores_fluxo / fatores_desconto).round(10)
    # Retorna a cotação (soma dos valores presentes) com truncamento ANBIMA
    return utils.truncar(vp.sum(), 6)


def pu(
    vna: float,
    cotacao: float,
) -> float:
    """
    Calcula o preço (PU) da NTN-B1 pelas regras do Tesouro Nacional.

    Args:
        vna (float): Valor nominal atualizado (VNA).
        cotacao (float): Cotação da NTN-B1 em base 100.

    Returns:
        float: Preço da NTN-B1 truncado em 6 casas decimais.

    References:
         - SEI Proccess 17944.005214/2024-09

    Examples:
        >>> from pyield import ntnb1
        >>> ntnb1.pu(4299.160173, 99.3651 / 100)
        4271.864805
        >>> ntnb1.pu(4315.498383, 100.6409 / 100)
        4343.156412
    """
    if any_is_empty(vna, cotacao):
        return float("nan")
    return utils.truncar(vna * cotacao, 6)


def duration(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
    nome_comercial: NomeComercial,
) -> float:
    """
    Calcula a Macaulay duration da NTN-B1 em anos úteis.

    Args:
        data_liquidacao (DateLike): Data de liquidação da operação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float): Taxa de desconto usada no cálculo.
        nome_comercial (NomeComercial): Nome comercial (Renda+ ou Educa+).

    Returns:
        float: Macaulay duration em anos úteis.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
        >>> ntnb1.duration("23-06-2025", "15-12-2084", 0.0686, r_mais)
        47.10493458167134
    """
    # Retorna NaN se houver entradas nulas
    if any_is_empty(data_liquidacao, data_vencimento, taxa, nome_comercial):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento, nome_comercial)
    anos_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"]) / 252
    vp = df_fluxos["valor_pagamento"] / (1 + taxa) ** anos_uteis
    duration = float((vp * anos_uteis).sum()) / float(vp.sum())

    # Trunca a duração para 14 casas para reprodutibilidade
    return utils.truncar(duration, 14)


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
    vna: float,
    nome_comercial: NomeComercial = NomeComercial.RENDA_MAIS,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B1 em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float): Taxa de desconto (YTM) da NTN-B1.
        vna (float): Valor nominal atualizado (VNA).
        nome_comercial (NomeComercial): Nome comercial (Renda+ ou Educa+).

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
        >>> ntnb1.dv01("23-06-2025", "15-12-2084", 0.0686, 4299.160173, r_mais)
        0.7738490000000127
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa, vna, nome_comercial):
        return float("nan")

    cotacao_1 = cotacao(data_liquidacao, data_vencimento, taxa, nome_comercial)
    cotacao_2 = cotacao(
        data_liquidacao,
        data_vencimento,
        taxa + 0.0001,
        nome_comercial,
    )
    pu_1 = pu(vna, cotacao_1)
    pu_2 = pu(vna, cotacao_2)
    return pu_1 - pu_2
