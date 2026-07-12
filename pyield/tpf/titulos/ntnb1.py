"""Precificação de NTN-B1 pelas regras do Tesouro Direto."""

from enum import Enum

import polars as pl

import pyield._internal.converters as conversores
from pyield import du, interpolador
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf.titulos import _utils as utils

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

    pagamento_amortizacao = utils.truncar(1 / numero_amortizacoes, 8)
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

    datas_pagamento = pl.Series(name="datas_pagamento", values=datas_amortizacao)

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
        │ 2060-12-15     ┆ 0.004168        │
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
    Calcula a cotação da NTN-B1 em base 1 pelo método do Tesouro Direto.

    Args:
        data_liquidacao (DateLike): Data de liquidação da operação.
        data_vencimento (DateLike): Data de vencimento da NTN-B1.
        taxa (float): Taxa de desconto (YTM) usada no valor presente.
        nome_comercial (NomeComercial): Nome comercial (Renda+ ou Educa+).

    Returns:
        float: Cotação da NTN-B1 em base 1, truncada em 6 casas decimais.

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
    # Na base 1, cada valor presente é arredondado na 12ª casa decimal.
    vp = (valores_fluxo / fatores_desconto).round(12)
    # Retorna a cotação em base 1, truncada na 6ª casa decimal.
    return utils.truncar(vp.sum(), 6)


def _validar_curva_zero(curva_zero: pl.DataFrame) -> pl.DataFrame:
    """Valida e normaliza a curva zero usada na precificação."""
    colunas_necessarias = {"dias_uteis", "taxa_zero"}
    colunas_ausentes = colunas_necessarias - set(curva_zero.columns)
    if colunas_ausentes:
        raise ValueError(
            "Curva zero deve conter as colunas 'dias_uteis' e 'taxa_zero'."
        )

    return (
        curva_zero.select(
            pl.col("dias_uteis").cast(pl.Int64),
            pl.col("taxa_zero").cast(pl.Float64),
        )
        .drop_nulls()
        .sort("dias_uteis")
    )


def _cotacao_por_taxas(pagamentos: pl.DataFrame) -> float:
    """
    Soma os valores presentes na precisão definida pelo método TD.

    Args:
        pagamentos: DataFrame com uma linha por fluxo e as colunas
            ``valor_pagamento`` (Float64), ``dias_uteis`` (Int64) e
            ``taxa`` (Float64) alinhadas por linha.
    """
    anos_uteis = utils.truncar(pagamentos["dias_uteis"] / 252, 14)
    fatores = (1 + pagamentos["taxa"]) ** anos_uteis
    valores_presentes = pagamentos["valor_pagamento"] / fatores
    return float(valores_presentes.round(12).sum())


def cotacao_curva_zero(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    curva_zero: pl.DataFrame,
    nome_comercial: NomeComercial,
) -> float:
    """
    Calcula a cotação de uma NTN-B1 descontando cada fluxo pela curva zero.

    A função usa interpolação flat-forward entre os vértices da curva e mantém
    a última taxa zero após o maior vértice, conforme a extrapolação do método
    TD. Cada valor presente, em base 1, é arredondado na 12ª casa decimal; a
    soma final não é truncada porque ela é o alvo da calibração da TIR
    equivalente.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data da última amortização da NTN-B1.
        curva_zero: DataFrame com as colunas ``dias_uteis`` e ``taxa_zero``.
        nome_comercial: Nome comercial, Renda+ ou Educa+.

    Returns:
        float: Cotação em base 1 calculada pela curva zero.
    """
    if any_is_empty(data_liquidacao, data_vencimento, nome_comercial):
        return float("nan")

    curva = _validar_curva_zero(curva_zero)
    fluxos = fluxos_caixa(data_liquidacao, data_vencimento, nome_comercial)
    dias_fluxos = du.contar(data_liquidacao, fluxos["data_pagamento"])
    taxas_fluxos = interpolador.interpolar(
        dias_fluxos,
        curva["dias_uteis"],
        curva["taxa_zero"],
        extrapolar=True,
    )
    pagamentos = fluxos.with_columns(dias_uteis=dias_fluxos, taxa=taxas_fluxos)
    return _cotacao_por_taxas(pagamentos)


def _resolver_taxa_equivalente(
    cotacao_alvo: float,
    pagamentos_base: pl.DataFrame,
    taxa_inicial: float,
) -> float:
    """Resolve por bisseção a taxa única que reproduz a cotação-alvo.

    Args:
        cotacao_alvo: Cotação em base 1 a ser reproduzida.
        pagamentos_base: DataFrame com as colunas ``valor_pagamento`` e
            ``dias_uteis`` de cada fluxo. A coluna ``taxa`` é adicionada
            a cada iteração.
        taxa_inicial: Estimativa inicial usada para dimensionar o limite
            superior da busca.
    """

    def erro(taxa: float) -> float:
        pagamentos = pagamentos_base.with_columns(taxa=pl.lit(taxa, dtype=pl.Float64))
        return _cotacao_por_taxas(pagamentos) - cotacao_alvo

    limite_inferior = -0.99
    limite_superior = max(1.0, 2 * taxa_inicial + 0.01)
    erro_inferior = erro(limite_inferior)
    erro_superior = erro(limite_superior)

    while erro_inferior * erro_superior > 0:
        limite_superior = 2 * limite_superior + 1
        erro_superior = erro(limite_superior)

    return utils._metodo_bissecao(
        erro,
        limite_inferior,
        limite_superior,
    )


def taxa_curva_zero(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    curva_zero: pl.DataFrame,
    nome_comercial: NomeComercial,
) -> float:
    """
    Calcula a TIR equivalente de uma NTN-B1 pela curva zero do método TD.

    Primeiro, cada amortização mensal do Renda+ ou Educa+ é descontada pela
    taxa zero correspondente à sua data. Em seguida, a função encontra por
    bisseção a taxa única que produz a mesma cotação quando aplicada a todos os
    fluxos. Essa é a taxa equivalente do título calculada pelo método TD.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data da última amortização da NTN-B1.
        curva_zero: DataFrame com as colunas ``dias_uteis`` e ``taxa_zero``.
        nome_comercial: Nome comercial, Renda+ ou Educa+.

    Returns:
        float: TIR equivalente anualizada, em formato decimal.
    """
    if any_is_empty(data_liquidacao, data_vencimento, nome_comercial):
        return float("nan")

    curva = _validar_curva_zero(curva_zero)
    fluxos = fluxos_caixa(data_liquidacao, data_vencimento, nome_comercial)
    dias_fluxos = du.contar(data_liquidacao, fluxos["data_pagamento"])
    taxas_zero = interpolador.interpolar(
        dias_fluxos,
        curva["dias_uteis"],
        curva["taxa_zero"],
        extrapolar=True,
    )
    pagamentos_base = fluxos.with_columns(dias_uteis=dias_fluxos)
    cotacao_alvo = _cotacao_por_taxas(pagamentos_base.with_columns(taxa=taxas_zero))
    return _resolver_taxa_equivalente(
        cotacao_alvo,
        pagamentos_base,
        taxa_inicial=float(taxas_zero[-1]),
    )


def pu(
    vna: float,
    cotacao: float,
) -> float:
    """
    Calcula o preço (PU) da NTN-B1 pelas regras do Tesouro Nacional.

    Args:
        vna (float): Valor nominal atualizado (VNA).
        cotacao (float): Cotação da NTN-B1 em base 1.

    Returns:
        float: Preço da NTN-B1 truncado em 6 casas decimais.

    References:
         - SEI Proccess 17944.005214/2024-09

    Examples:
        >>> from pyield import ntnb1
        >>> ntnb1.pu(4299.160173, 0.993651)
        4271.864805
        >>> ntnb1.pu(4315.498383, 1.006409)
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
        47.10494386899197
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
    pu: float,
    nome_comercial: NomeComercial = NomeComercial.RENDA_MAIS,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B1 em R$.

    Representa a variação do PU informado para um aumento de 1 bp (0,01%) na
    taxa.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float): Taxa de desconto (YTM) da NTN-B1.
        pu (float): PU usado como base para o cálculo.
        nome_comercial (NomeComercial): Nome comercial (Renda+ ou Educa+).

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnb1
        >>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
        >>> cot = ntnb1.cotacao("23-06-2025", "15-12-2084", 0.0686, r_mais)
        >>> pu = ntnb1.pu(4299.160173, cot)
        >>> ntnb1.dv01("23-06-2025", "15-12-2084", 0.0686, pu, r_mais)
        0.7738488291718512
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa, pu, nome_comercial):
        return float("nan")

    cotacao_1 = cotacao(data_liquidacao, data_vencimento, taxa, nome_comercial)
    cotacao_2 = cotacao(
        data_liquidacao,
        data_vencimento,
        taxa + 0.0001,
        nome_comercial,
    )
    return pu * (1 - cotacao_2 / cotacao_1)
