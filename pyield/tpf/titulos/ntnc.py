"""Cálculos e dados de NTN-C.

Convenções de precificação (STN, tabela 3):
    - Cotação em base 100, truncada a 4 casas.
    - VNA recebido no cálculo do PU: truncado a 6 casas.
    - Prazo de desconto: dias úteis / 252, truncado a 14 casas.
    - PU: truncado a 6 casas.
    - Taxa implícita retornada: decimal, truncada a 8 casas,
      equivalente a 6 casas em termos percentuais.
    - Cada fluxo descontado: arredondado a 10 casas.

Valores de referência derivados conforme as regras da STN, exibidos em base 100.
Para NTN-C com vencimento em 01-01-2031:
    PRINCIPAL = 100
    TAXA_CUPOM = ((0.12 + 1) ** 0.5 - 1) * 100  # 12% a.a. com capitalização semestral
    VALOR_CUPOM_2031 = round(TAXA_CUPOM, 6) -> 5.830052
    VALOR_FINAL_2031 = 105.830052

Para as demais NTN-C:
    TAXA_CUPOM = ((0.06 + 1) ** 0.5 - 1) * 100  # 6% a.a. com capitalização semestral
    VALOR_CUPOM = round(TAXA_CUPOM, 6) -> 2.956301
    VALOR_FINAL = 102.956301
"""

import datetime as dt
import math
from decimal import Decimal

import polars as pl

import pyield._internal.converters as conversores
from pyield import du, interpolador
from pyield._internal.numbers import truncar_decimal
from pyield._internal.types import DateLike, any_is_empty

from . import _utils

# Valores usados nos cálculos, em base 100, com 6 casas decimais
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
    df = _utils.obter_tpf(data, "NTN-C")
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
    df = _utils.adicionar_taxa_di(df, data)

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
    e a amortização do principal. A NTN-C é definida pela data de vencimento. As
    datas são contratuais e não são ajustadas para dias úteis.

    Args:
        data_liquidacao: Data de liquidação (exclusiva).
        data_vencimento: Data de vencimento.

    Returns:
        pl.Series: Série de datas de pagamento entre a liquidação (exclusiva)
            e o vencimento (inclusivo). Retorna série vazia se o vencimento for
            menor ou igual à liquidação.

    Notes:
        Para obter as datas efetivas de processamento, use
        ``yd.du.deslocar(..., 0)``.

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
    return _utils.gerar_datas_pagamento(data_liquidacao, data_vencimento)


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
        - data_pagamento (Date): Data contratual do pagamento, sem ajuste para
            dia útil.
        - valor_pagamento (Float64): Valor do pagamento em base 100.

    Notes:
        Para obter as datas efetivas de processamento, use
        ``yd.du.deslocar(..., 0)``.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.fluxos_caixa("21-03-2025", "01-01-2031")
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
    taxa: float | Decimal | str,
) -> Decimal:
    """
    Calcula a cotação da NTN-C pela metodologia da STN para leilões primários.

    Args:
        data_liquidacao: Data de liquidação da operação.
        data_vencimento: Data de vencimento da NTN-C.
        taxa: Taxa de desconto (YTM) em formato decimal.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
            Antes do cálculo, é truncada em oito casas decimais (seis na
            forma percentual), descartando as casas excedentes sem arredondar.

    Returns:
        Decimal: Cotação em base 100, truncada em 4 casas decimais.

    Notes:
        A cotação é calculada e retornada na escala percentual (base 100),
        truncada em 4 casas conforme a STN. Por exemplo, ``99.3651`` representa
        99,3651% do VNA; o PU é calculado como ``VNA * cotacao / 100``.

        Os cupons semestrais são armazenados em base 100, com 6 casas
        decimais. Cada fluxo descontado é arredondado em 10 casas antes da soma.

    References:
        - Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos
          Públicos Federais Ofertados nos Leilões Primários.
          https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.cotacao("21-03-2025", "01-01-2031", "6.7626%")
        Decimal('126.4958')
    """
    taxa = _utils.converter_taxa(taxa)
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return Decimal("NaN")
    taxa = _utils.normalizar_taxa_precificacao(taxa)

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return Decimal("NaN")

    valores_fluxo = df_fluxos["valor_pagamento"]
    dias_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"])
    anos_uteis = _utils.truncar(dias_uteis / 252, 14)
    fatores_desconto = (1 + taxa) ** anos_uteis
    # Calcula o valor presente de cada fluxo com arredondamento STN
    vp = (valores_fluxo / fatores_desconto).round(10)
    # Retorna a cotação (soma dos valores presentes) com truncamento STN
    # Soma decimal preserva os fluxos arredondados no limite do truncamento.
    cotacao_total = sum(Decimal(str(valor)) for valor in vp)
    return truncar_decimal(cotacao_total, 4)


def _pagamentos_curva_zero(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    curva_zero: pl.DataFrame,
) -> pl.DataFrame:
    """Prepara fluxos e taxas da curva fornecida, sem escolher seu indexador."""
    if not {"dias_uteis", "taxa_zero"}.issubset(curva_zero.columns):
        raise ValueError(
            "Curva zero deve conter as colunas 'dias_uteis' e 'taxa_zero'."
        )
    curva = curva_zero.select(
        pl.col("dias_uteis").cast(pl.Float64),
        pl.col("taxa_zero").cast(pl.Float64),
    )
    if (
        curva.is_empty()
        or curva.select(
            (
                pl.col("dias_uteis").is_null()
                | ~pl.col("dias_uteis").is_finite()
                | (pl.col("dias_uteis") <= 0)
                | (pl.col("dias_uteis") != pl.col("dias_uteis").floor())
                | pl.col("taxa_zero").is_null()
                | ~pl.col("taxa_zero").is_finite()
                | (pl.col("taxa_zero") <= -1)
            ).any()
        ).item()
    ):
        raise ValueError(
            "Curva deve ter prazos inteiros positivos e taxas finitas > -1."
        )
    if curva["dias_uteis"].n_unique() != curva.height:
        raise ValueError("Curva zero não pode conter prazos duplicados.")

    fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    dias = du.contar(data_liquidacao, fluxos["data_pagamento"])
    taxas = interpolador.interpolar(
        dias, curva["dias_uteis"].cast(pl.Int64), curva["taxa_zero"], extrapolar=True
    )
    return fluxos.with_columns(dias_uteis=dias, taxa=taxas)


def cotacao_curva_zero(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    curva_zero: pl.DataFrame,
) -> float:
    """Calcula a cotação da NTN-C descontando seus fluxos pela curva informada.

    Args:
        data_liquidacao: Data de liquidação, exclusiva para seleção dos fluxos.
        data_vencimento: Data de vencimento da NTN-C.
        curva_zero: DataFrame com ``dias_uteis`` (prazos inteiros positivos,
            únicos) e ``taxa_zero`` (taxas anuais decimais finitas maiores que -1).
            A curva deve conter ao menos um vértice; a ordem é indiferente.

    Returns:
        float: Cotação em base 100, sem truncamento final. Retorna NaN para
            datas ausentes, ausência de fluxos ou resultado não finito.

    Raises:
        ValueError: Curva vazia, colunas ausentes ou vértices inválidos.

    Notes:
        Usa os fluxos contratuais de ``fluxos_caixa``, sem deslocar suas datas.
        O prazo em dias úteis / 252 é truncado em 14 casas. A interpolação é
        flat-forward, com taxa constante nas duas pontas da curva. Cada valor
        presente é arredondado em 10 casas, como em ``cotacao``; a soma não é
        truncada, pois serve de alvo para a TIR equivalente.

        A curva é uma hipótese do consumidor. A biblioteca não escolhe uma
        curva de IPCA como aproximação para IGP-M nem aplica piso ANBIMA.
        Esta operação não representa uma cotação consultada em fonte externa.
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return float("nan")
    pagamentos = _pagamentos_curva_zero(data_liquidacao, data_vencimento, curva_zero)
    if pagamentos.is_empty():
        return float("nan")
    cotacao = _utils.cotacao_por_taxas(pagamentos)
    return cotacao if math.isfinite(cotacao) else float("nan")


def taxa_curva_zero(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    curva_zero: pl.DataFrame,
) -> float:
    """Calcula a TIR equivalente à cotação da NTN-C pela curva zero informada.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento da NTN-C.
        curva_zero: DataFrame com ``dias_uteis`` e ``taxa_zero``, com os mesmos
            requisitos de ``cotacao_curva_zero``.

    Returns:
        float: TIR anualizada decimal, sem arredondamento comercial. Retorna
            NaN para datas ausentes, ausência de fluxos com prazo positivo,
            resultado não finito ou falha de convergência.

    Raises:
        ValueError: Curva vazia, colunas ausentes ou vértices inválidos.

    Notes:
        Usa datas, interpolação e precisão de ``cotacao_curva_zero``. Resolve
        por bisseção a taxa única que reproduz sua cotação, sem truncar a taxa
        durante a busca. Não inverte um PU nem a cotação truncada de ``cotacao``.
        O intervalo é delimitado pelas menores e maiores taxas interpoladas
        dos fluxos. A busca para por erro de cotação zero ou meia largura do
        intervalo inferior a 1e-12, com limite de 100 iterações.
        A escolha da curva e eventuais spreads ou pisos ficam com o consumidor.
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return float("nan")
    pagamentos = _pagamentos_curva_zero(data_liquidacao, data_vencimento, curva_zero)
    if pagamentos.is_empty() or not (pagamentos["dias_uteis"] > 0).any():
        return float("nan")
    alvo = _utils.cotacao_por_taxas(pagamentos)
    if not math.isfinite(alvo):
        return float("nan")

    def erro(taxa: float) -> float:
        return (
            _utils.cotacao_por_taxas(pagamentos.with_columns(taxa=pl.lit(taxa))) - alvo
        )

    taxas = pagamentos["taxa"].sort()
    return _utils.encontrar_raiz(erro, intervalo=(float(taxas[0]), float(taxas[-1])))


def _calcular_pu(
    vna: float | Decimal,
    cotacao: float | Decimal,
) -> Decimal:
    """Calcula o preço unitário da NTN-C a partir do VNA e da cotação."""
    if any_is_empty(vna, cotacao):
        return Decimal("NaN")
    vna_decimal = truncar_decimal(vna, 6)
    cotacao_decimal = truncar_decimal(cotacao, 4)
    return truncar_decimal(vna_decimal * cotacao_decimal / 100, 6)


def pu(
    vna: float | Decimal,
    cotacao: float | Decimal,
) -> Decimal:
    """
    Calcula o PU da NTN-C pela metodologia da STN para leilões primários.

    pu = VNA * cotacao / 100

    Args:
        vna: Valor nominal atualizado (VNA), truncado em seis casas decimais
            antes do cálculo, sem arredondar.
        cotacao: Cotação da NTN-C em base 100.
            Truncada em quatro casas decimais antes do cálculo, sem arredondar.

    Returns:
        Decimal: Preço da NTN-C truncado em 6 casas decimais.

    References:
        - Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos
          Públicos Federais Ofertados nos Leilões Primários.
          https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.pu(6598.913723, 126.4958)
        Decimal('8347.348705')
    """
    return _calcular_pu(vna, cotacao)


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    vna: float | Decimal,
    pu: float | Decimal,
) -> Decimal:
    """
    Calcula a taxa implícita (YTM) de uma NTN-C a partir do preço (PU).

    A função inverte numericamente a cadeia ``pu(vna, cotacao(...))``,
    encontrando a taxa que zera a diferença entre o preço calculado e o
    informado.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        vna: Valor nominal atualizado (VNA).
        pu: Preço unitário (PU) do título.

    Returns:
        Decimal: Taxa implícita (YTM) em formato decimal, truncada em oito
            casas decimais (seis casas em termos percentuais). Retorna
            ``Decimal("NaN")`` para entradas ausentes, PU não positivo ou
            falha na resolução numérica.

    Examples:
        Exibe as taxas em formato decimal:

        >>> from pyield import ntnc
        >>> ntnc.taxa("21-03-2025", "01-01-2031", 6598.913723, 8347.348705)
        Decimal('0.06762593')
        >>> ntnc.taxa("21-05-2008", "01-03-2011", 2126.473734, 2207.556177) * 100
        Decimal('4.98769500')
    """
    if any_is_empty(data_liquidacao, data_vencimento, vna, pu):
        return Decimal("NaN")

    pu_float = float(pu)
    if pu_float <= 0:
        return Decimal("NaN")

    def diferenca_preco(taxa: float) -> float:
        cotacao_calc = cotacao(data_liquidacao, data_vencimento, taxa)
        return float(_calcular_pu(vna, cotacao_calc)) - pu_float

    taxa_encontrada = _utils.encontrar_raiz(diferenca_preco)
    return truncar_decimal(taxa_encontrada, 8)


def duration(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float | str,
) -> float:
    """
    Calcula a Macaulay duration da NTN-C em anos úteis.

    Args:
        data_liquidacao: Data de liquidação da operação.
        data_vencimento: Data de vencimento.
        taxa: Taxa de desconto usada no cálculo.
            Aceita também percentual explícito: "5.75%" ou "5,75%".

    Returns:
        float: Macaulay duration em anos úteis.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.duration("21-03-2025", "01-01-2031", "6.7626%")
        4.405363320448
    """
    if isinstance(taxa, str):
        taxa = float(_utils.converter_taxa(taxa))
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    anos_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"]) / 252
    vp = df_fluxos["valor_pagamento"] / (1 + taxa) ** anos_uteis
    duracao = float((vp * anos_uteis).sum()) / float(vp.sum())
    # Truncar para 14 casas decimais para repetibilidade dos resultados
    return _utils.truncar(duracao, 14)


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
        _utils.coluna_ou_expr(data_liquidacao, "data_liquidacao"),
        _utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        _utils.coluna_ou_expr(taxa, "taxa"),
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
    taxa: float | Decimal | str,
    pu: float | Decimal,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-C em R$.

    Representa a variação do PU informado para um aumento de 1 bp (0,01%) na
    taxa.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        taxa: Taxa de desconto (YTM) da NTN-C.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
        pu: PU usado como base para o cálculo.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnc
        >>> cot = ntnc.cotacao("21-03-2025", "01-01-2031", "6.7626%")
        >>> pu = ntnc.pu(6598.913723, cot)
        >>> ntnc.dv01("21-03-2025", "01-01-2031", "6.7626%", pu)
        3.444632963315593
    """
    taxa = _utils.converter_taxa(taxa)
    if any_is_empty(data_liquidacao, data_vencimento, taxa, pu):
        return float("nan")

    taxa = _utils.normalizar_taxa_precificacao(taxa)
    taxa_mais_1bp = round(taxa + 0.0001, 8)
    cotacao_1 = cotacao(data_liquidacao, data_vencimento, taxa)
    cotacao_2 = cotacao(data_liquidacao, data_vencimento, taxa_mais_1bp)
    fator_variacao = 1 - float(cotacao_2) / float(cotacao_1)
    return float(pu) * fator_variacao


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
        _utils.coluna_ou_expr(data_liquidacao, "data_liquidacao"),
        _utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        _utils.coluna_ou_expr(taxa, "taxa"),
        _utils.coluna_ou_expr(pu, "pu"),
    ).map_elements(
        lambda s: dv01(
            s["data_liquidacao"],
            s["data_vencimento"],
            s["taxa"],
            s["pu"],
        ),
        return_dtype=pl.Float64,
    )
