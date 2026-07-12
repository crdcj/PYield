import datetime as dt

import polars as pl

import pyield._internal.converters as conversores
from pyield import du
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf.titulos import _utils as utils

"""
Constantes calculadas conforme regras da ANBIMA e em base 1.
Válido para NTN-C com vencimento 01-01-2031:
PRINCIPAL = 1
TAXA_CUPOM = (0.12 + 1) ** 0.5 - 1  # 12% a.a. com capitalização semestral
VALOR_CUPOM_2031 = round(TAXA_CUPOM, 8) -> 0.05830052
VALOR_FINAL_2031 = principal + último cupom = 1 + 0.05830052

Para as demais NTN-C:
TAXA_CUPOM = (0.06 + 1) ** 0.5 - 1  # 6% a.a. com capitalização semestral
VALOR_CUPOM = round(TAXA_CUPOM, 8) -> 0.02956301
VALOR_FINAL = principal + último cupom = 1 + 0.02956301
"""
VALOR_CUPOM_2031 = 0.05830052
VALOR_FINAL_2031 = 1.05830052

VALOR_CUPOM = 0.02956301
VALOR_FINAL = 1.02956301


def _obter_valor_cupom(vencimento: dt.date) -> float:
    if vencimento.year == 2031:  # noqa
        return VALOR_CUPOM_2031
    return VALOR_CUPOM


def _obter_valor_final(vencimento: dt.date) -> float:
    if vencimento.year == 2031:  # noqa
        return VALOR_FINAL_2031
    return VALOR_FINAL


def dados(data: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de NTN-C para a data de referência.

    Args:
        data: Data da consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de NTN-C.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "NTN-C").
        - codigo_selic (Int64): Código do título no SELIC.
        - data_base (Date): Data base de emissão do título.
        - data_vencimento (Date): Data de vencimento do título.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - duration (Float64): Macaulay Duration do título (anos).
        - prazo_medio (Float64): Prazo médio do título (anos).
        - dv01 (Float64): Variação no preço para 1bp de taxa.
        - pu (Float64): Preço unitário (PU).
        - taxa_compra (Float64): Taxa de compra (decimal).
        - taxa_venda (Float64): Taxa de venda (decimal).
        - taxa_indicativa (Float64): Taxa indicativa (decimal).
        - taxa_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo
            método flat forward.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.dados("23-08-2024")  # doctest: +SKIP
    """
    df = utils.obter_tpf(data, "NTN-C")
    if df.is_empty():
        return df

    # Adiciona duration, prazo_medio, dv01 e taxa_di
    df = df.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
        duration=duration_expr("data_referencia", "data_vencimento", "taxa_indicativa"),
    ).with_columns(
        prazo_medio=pl.col("duration"),
        dv01=dv01_expr("data_referencia", "data_vencimento", "taxa_indicativa", "pu"),
    )
    df = utils.adicionar_taxa_di(df, data)

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
        "pu",
        "taxa_compra",
        "taxa_venda",
        "taxa_indicativa",
        "taxa_di",
    )


def datas_pagamento(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
) -> pl.Series:
    """
    Gera todas as datas de pagamento entre liquidação e vencimento.

    Os pagamentos são semestrais. No vencimento, o fluxo inclui o último cupom
    e a amortização do principal. A NTN-C é definida pela data de vencimento.

    Args:
        data_liquidacao: Data de liquidação (exclusiva).
        data_vencimento: Data de vencimento.

    Returns:
        pl.Series: Série de datas de pagamento entre a liquidação (exclusiva)
            e o vencimento (inclusivo). Retorna série vazia se o vencimento for
            menor ou igual à liquidação.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.datas_pagamento("21-03-2025", "01-01-2031")
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
    return utils.gerar_datas_pagamento(data_liquidacao, data_vencimento)


def fluxos_caixa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
) -> pl.DataFrame:
    """
    Gera os fluxos de caixa da NTN-C entre liquidação e vencimento.

    Args:
        data_liquidacao: Data de liquidação (exclusiva).
        data_vencimento: Data de vencimento.

    Returns:
        pl.DataFrame: DataFrame com as colunas de fluxo.

    Output Columns:
        - data_pagamento (Date): Data de pagamento.
        - valor_pagamento (Float64): Valor do pagamento em base 1.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.fluxos_caixa("21-03-2025", "01-01-2031")
        shape: (12, 2)
        ┌────────────────┬─────────────────┐
        │ data_pagamento ┆ valor_pagamento │
        │ ---            ┆ ---             │
        │ date           ┆ f64             │
        ╞════════════════╪═════════════════╡
        │ 2025-07-01     ┆ 0.058301        │
        │ 2026-01-01     ┆ 0.058301        │
        │ 2026-07-01     ┆ 0.058301        │
        │ 2027-01-01     ┆ 0.058301        │
        │ 2027-07-01     ┆ 0.058301        │
        │ …              ┆ …               │
        │ 2029-01-01     ┆ 0.058301        │
        │ 2029-07-01     ┆ 0.058301        │
        │ 2030-01-01     ┆ 0.058301        │
        │ 2030-07-01     ┆ 0.058301        │
        │ 2031-01-01     ┆ 1.058301        │
        └────────────────┴─────────────────┘
    """
    vazio = pl.DataFrame(
        schema={"data_pagamento": pl.Date, "valor_pagamento": pl.Float64}
    )

    if any_is_empty(data_liquidacao, data_vencimento):
        return vazio

    # Valida e normaliza datas
    liquidacao = conversores.converter_datas(data_liquidacao)
    vencimento = conversores.converter_datas(data_vencimento)

    # Obtém as datas de pagamento entre liquidação e vencimento
    serie_datas_pagamento = datas_pagamento(liquidacao, vencimento)

    # Retorna DataFrame vazio se não houver pagamentos (liquidação >= vencimento)
    if serie_datas_pagamento.is_empty():
        return vazio

    # Obtém os valores corretos de cupom e final
    valor_cupom = _obter_valor_cupom(vencimento)
    valor_final = _obter_valor_final(vencimento)

    # Monta DataFrame com fluxos de caixa
    df = pl.DataFrame({"data_pagamento": serie_datas_pagamento}).with_columns(
        pl.when(pl.col("data_pagamento") == vencimento)
        .then(valor_final)
        .otherwise(valor_cupom)
        .alias("valor_pagamento")
    )
    return df


def cotacao(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    """
    Calcula a cotação da NTN-C em base 1 pelas regras da ANBIMA.

    Args:
        data_liquidacao: Data de liquidação da operação.
        data_vencimento: Data de vencimento da NTN-C.
        taxa: Taxa de desconto (YTM) usada no valor presente.

    Returns:
        float: Cotação da NTN-C truncada em 6 casas decimais.

    Notes:
        A ANBIMA apresenta a cotação na escala percentual (base 100). Esta
        função retorna o fator equivalente em base 1, usado diretamente no
        cálculo do PU. O truncamento de 4 casas na escala ANBIMA equivale ao
        truncamento de 6 casas nesta representação.

        Os cupons semestrais divulgados como percentuais também são armazenados
        como fatores em base 1.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.cotacao("21-03-2025", "01-01-2031", 0.067626)
        1.264958
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    valores_fluxo = df_fluxos["valor_pagamento"]
    dias_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"])
    anos_uteis = utils.truncar(dias_uteis / 252, 14)
    fatores_desconto = (1 + taxa) ** anos_uteis
    # Calcula o valor presente de cada fluxo com arredondamento ANBIMA
    vp = (valores_fluxo / fatores_desconto).round(12)
    # Retorna a cotação (soma dos valores presentes) com truncamento ANBIMA
    return utils.truncar(vp.sum(), 6)


def _calcular_pu(
    vna: float,
    cotacao: float,
) -> float:
    """Calcula o preço unitário da NTN-C a partir do VNA e da cotação."""
    if any_is_empty(vna, cotacao):
        return float("nan")
    return utils.truncar(vna * cotacao, 6)


def pu(
    vna: float,
    cotacao: float,
) -> float:
    """
    Calcula o preço (PU) da NTN-C pelas regras da ANBIMA.

    pu = VNA * cotacao

    Args:
        vna (float): Valor nominal atualizado (VNA).
        cotacao (float): Cotação da NTN-C em base 1.

    Returns:
        float: Preço da NTN-C truncado em 6 casas decimais.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.pu(6598.913723, 1.264958)
        8347.348705
    """
    return _calcular_pu(vna, cotacao)


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    vna: float,
    pu: float,
) -> float:
    """
    Calcula a taxa implícita (YTM) de uma NTN-C a partir do preço (PU).

    A função inverte numericamente a cadeia ``pu(vna, cotacao(...))``,
    encontrando a taxa que zera a diferença entre o preço calculado e o
    informado.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        vna (float): Valor nominal atualizado (VNA).
        pu (float): Preço unitário (PU) do título.

    Returns:
        float: Taxa implícita (YTM) em formato decimal. Retorna NaN em
            caso de erro.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.taxa("21-03-2025", "01-01-2031", 6598.913723, 8347.348705)
        0.067626
    """
    if any_is_empty(data_liquidacao, data_vencimento, vna, pu):
        return float("nan")

    if pu <= 0:
        return float("nan")

    def diferenca_preco(taxa: float) -> float:
        cotacao_calc = cotacao(data_liquidacao, data_vencimento, taxa)
        return _calcular_pu(vna, cotacao_calc) - pu

    taxa_encontrada = utils.encontrar_raiz(diferenca_preco)
    return round(taxa_encontrada, 6)


def duration(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    """
    Calcula a Macaulay duration da NTN-C em anos úteis.

    Args:
        data_liquidacao: Data de liquidação da operação.
        data_vencimento: Data de vencimento.
        taxa: Taxa de desconto usada no cálculo.

    Returns:
        float: Macaulay duration em anos úteis.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.duration("21-03-2025", "01-01-2031", 0.067626)
        4.405363320448
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    anos_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"]) / 252
    vp = df_fluxos["valor_pagamento"] / (1 + taxa) ** anos_uteis
    duracao = float((vp * anos_uteis).sum()) / float(vp.sum())
    # Truncar para 14 casas decimais para repetibilidade dos resultados
    return utils.truncar(duracao, 14)


def duration_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
    taxa: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para a duration da NTN-C.

    O cálculo é aplicado linha a linha porque a duration depende dos fluxos de
    caixa do título.

    Args:
        data_liquidacao: Nome de coluna ou expressão Polars com a data de
            liquidação.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento.
        taxa: Nome de coluna ou expressão Polars com a taxa em formato decimal.

    Returns:
        pl.Expr: Expressão sem alias com a Macaulay duration em anos úteis.
    """
    return pl.struct(
        utils.coluna_ou_expr(data_liquidacao, "data_liquidacao"),
        utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        utils.coluna_ou_expr(taxa, "taxa"),
    ).map_elements(
        lambda s: duration(
            s["data_liquidacao"],
            s["data_vencimento"],
            s["taxa"],
        ),
        return_dtype=pl.Float64,
    )


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
    pu: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-C em R$.

    Representa a variação do PU informado para um aumento de 1 bp (0,01%) na
    taxa.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        taxa: Taxa de desconto (YTM) da NTN-C.
        pu: PU usado como base para o cálculo.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnc
        >>> cot = ntnc.cotacao("21-03-2025", "01-01-2031", 0.067626)
        >>> pu = ntnc.pu(6598.913723, cot)
        >>> ntnc.dv01("21-03-2025", "01-01-2031", 0.067626, pu)
        3.4446329633165194
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa, pu):
        return float("nan")

    cotacao_1 = cotacao(data_liquidacao, data_vencimento, taxa)
    cotacao_2 = cotacao(data_liquidacao, data_vencimento, taxa + 0.0001)
    return pu * (1 - cotacao_2 / cotacao_1)


def dv01_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
    taxa: pl.Expr | str,
    pu: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para o DV01 da NTN-C.

    O cálculo é aplicado linha a linha e reprifica o PU informado para um
    aumento de 1 bp na taxa.

    Args:
        data_liquidacao: Nome de coluna ou expressão Polars com a data de
            liquidação.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento.
        taxa: Nome de coluna ou expressão Polars com a taxa em formato decimal.
        pu: Nome de coluna ou expressão Polars com o PU usado como base.

    Returns:
        pl.Expr: Expressão sem alias com o DV01.
    """
    return pl.struct(
        utils.coluna_ou_expr(data_liquidacao, "data_liquidacao"),
        utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        utils.coluna_ou_expr(taxa, "taxa"),
        utils.coluna_ou_expr(pu, "pu"),
    ).map_elements(
        lambda s: dv01(
            s["data_liquidacao"],
            s["data_vencimento"],
            s["taxa"],
            s["pu"],
        ),
        return_dtype=pl.Float64,
    )
