"""Cálculos e dados de LFT.

Convenções de precificação (STN, tabela 3):
    - Cotação em base 100, truncada a 4 casas.
    - VNA recebido no cálculo do PU: truncado a 6 casas.
    - Prazo de desconto: dias úteis / 252, truncado a 14 casas.
    - PU: truncado a 6 casas.
    - Taxa implícita retornada: decimal, truncada a 8 casas,
      equivalente a 6 casas em termos percentuais.
"""

from decimal import Decimal

import polars as pl

from pyield import du
from pyield._internal.numbers import truncar_decimal
from pyield._internal.types import DateLike, any_is_empty

from . import _utils

BASE_COTACAO = 100


def dados(data: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de LFT para a data de referência na ANBIMA.

    Args:
        data: Data da consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de LFT.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "LFT").
        - codigo_selic (Int64): Código do título no SELIC.
        - data_base (Date): Data base de emissão do título.
        - data_vencimento (Date): Data de vencimento do título.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - prazo_medio (Float64): Prazo médio do título em anos.
        - pu (Float64): Preço unitário (PU).
        - taxa_compra (Float64): Taxa de compra (decimal).
        - taxa_venda (Float64): Taxa de venda (decimal).
        - taxa_indicativa (Float64): Taxa indicativa (decimal).
        - taxa_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo
            método flat forward.
        - rentabilidade (Float64): Rentabilidade da LFT sobre o DI.

    Examples:
        >>> from pyield import lft
        >>> df_lft = lft.dados("23-08-2024")  # doctest: +SKIP
    """
    df = _utils.obter_tpf(data, "LFT")
    if df.is_empty():
        return df

    df = df.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
    ).with_columns(
        prazo_medio=pl.col("dias_uteis") / 252,
    )
    df = _utils.adicionar_taxa_di(df, data)

    df = df.with_columns(
        rentabilidade=rentabilidade_expr("taxa_indicativa", "taxa_di"),
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


def vencimentos(data: DateLike) -> pl.Series:
    """
    Busca os vencimentos disponíveis para a data de referência.

    Args:
        data: Data da consulta.

    Returns:
        pl.Series: Série de datas de vencimento disponíveis.

    Examples:
        >>> from pyield import lft
        >>> lft.vencimentos("22-08-2024")
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
    return dados(data)["data_vencimento"]


def cotacao(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float | Decimal | str,
) -> Decimal:
    """
    Calcula a cotação de uma LFT pela metodologia da STN para leilões primários.

    Args:
        data_liquidacao: Data de liquidação do título.
        data_vencimento: Data de vencimento do título.
        taxa: Taxa anualizada do título em formato decimal.
            Aceita também percentual explícito: "5.75%" ou "5,75%".

    Returns:
        Decimal: Cotação em base 100, truncada em 4 casas decimais.
            Retorna ``Decimal("NaN")`` quando o prazo até o vencimento não é
            positivo.

    Notes:
        A cotação é calculada e retornada na escala percentual (base 100),
        truncada em 4 casas conforme a STN. Por exemplo, ``99.3651`` representa
        99,3651% do VNA; o PU é calculado como ``VNA * cotacao / 100``.

    Examples:
        Calcula a cotação de uma LFT com taxa de 0,1717%:
        >>> from pyield import lft
        >>> lft.cotacao(
        ...     data_liquidacao="24-07-2024",
        ...     data_vencimento="01-09-2030",
        ...     taxa="0.1717%",
        ... )
        Decimal('98.9645')
        >>> lft.cotacao("21-05-2008", "07-03-2014", "-0.0200009%")
        Decimal('100.1158')

        Entradas nulas retornam Decimal('NaN'):
        >>> lft.cotacao(
        ...     data_liquidacao=None, data_vencimento="01-09-2030", taxa="0.1717%"
        ... )
        Decimal('NaN')
    """
    taxa = _utils.converter_taxa(taxa)
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return Decimal("NaN")
    taxa = _utils.normalizar_taxa_precificacao(taxa)
    # Número de dias úteis entre liquidação (inclusivo) e vencimento (exclusivo)
    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return Decimal("NaN")

    # Número de períodos truncado conforme regras da STN
    anos_truncados = _utils.truncar(dias_uteis / 252, 14)

    cotacao_percentual = BASE_COTACAO / (1 + taxa) ** anos_truncados

    return truncar_decimal(cotacao_percentual, 4)


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    vna: float | Decimal,
    pu: float | Decimal,
) -> Decimal:
    """
    Calcula a taxa implícita de uma LFT a partir do preço (PU).

    A função inverte numericamente a cadeia ``pu(vna, cotacao(...))``,
    encontrando a taxa que zera a diferença entre o preço calculado e o
    informado.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        vna: Valor nominal atualizado (VNA).
        pu: Preço unitário (PU) do título.

    Returns:
        Decimal: Taxa implícita em formato decimal, truncada em oito casas
            decimais (seis casas em termos percentuais). Retorna
            ``Decimal("NaN")`` para entradas ausentes, PU não positivo, prazo
            útil não positivo ou falha na resolução numérica.

    Examples:
        Exibe as taxas em formato decimal:

        >>> from pyield import lft
        >>> lft.taxa("24-07-2024", "01-09-2030", 15785.324502, 15621.867466)
        Decimal('0.00171691')
        >>> lft.taxa("24-07-2024", "01-03-2025", 15785.324502, 15774.132706) * 100
        Decimal('0.11596600')
        >>> lft.taxa("21-05-2008", "07-03-2014", 3451.215345, 3426.649594) * 100
        Decimal('0.12344300')
    """
    if any_is_empty(data_liquidacao, data_vencimento, vna, pu):
        return Decimal("NaN")

    pu_float = float(pu)
    if pu_float <= 0:
        return Decimal("NaN")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return Decimal("NaN")

    def diferenca_preco(taxa: float) -> float:
        preco = _calcular_pu(vna, cotacao(data_liquidacao, data_vencimento, taxa))
        return float(preco) - pu_float

    taxa_encontrada = _utils.encontrar_raiz(diferenca_preco)
    return truncar_decimal(taxa_encontrada, 8)


def rentabilidade(taxa_lft: float | str, taxa_di: float | str) -> float:
    """
    Calcula a rentabilidade da LFT sobre a taxa de DI Futuro.

    Args:
        taxa_lft: Taxa anualizada da LFT sobre a Selic.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
        taxa_di: Taxa DI Futuro anualizada (interpolada para o mesmo
            vencimento da LFT).
            Aceita também percentual explícito: "5.75%" ou "5,75%".

    Returns:
        float: Rentabilidade da LFT sobre o DI.

    Examples:
        Calcula a rentabilidade de uma LFT em 28/04/2025:
        >>> from pyield import lft
        >>> taxa_lft = "0.1124%"
        >>> taxa_di = "13.967670224373396%"
        >>> lft.rentabilidade(taxa_lft, taxa_di)
        1.008594331960501
    """
    if isinstance(taxa_lft, str):
        taxa_lft = float(_utils.converter_taxa(taxa_lft))
    if isinstance(taxa_di, str):
        taxa_di = float(_utils.converter_taxa(taxa_di))
    if any_is_empty(taxa_lft, taxa_di):
        return float("nan")
    # Taxa diária
    fator_lft = (taxa_lft + 1) ** (1 / 252)
    fator_di = (taxa_di + 1) ** (1 / 252)
    return (fator_lft * fator_di - 1) / (fator_di - 1)


def rentabilidade_expr(
    taxa_lft: pl.Expr | str,
    taxa_di: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para a rentabilidade da LFT sobre o DI.

    Args:
        taxa_lft: Nome de coluna ou expressão Polars com a taxa anualizada da
            LFT sobre a Selic.
        taxa_di: Nome de coluna ou expressão Polars com a taxa DI Futuro
            anualizada (interpolada para o mesmo vencimento da LFT).

    Returns:
        pl.Expr: Expressão sem alias com a rentabilidade da LFT sobre o DI.
    """
    expr_lft = taxa_lft if isinstance(taxa_lft, pl.Expr) else pl.col(taxa_lft)
    expr_di = taxa_di if isinstance(taxa_di, pl.Expr) else pl.col(taxa_di)
    fator_lft = (expr_lft + 1) ** (1 / 252)
    fator_di = (expr_di + 1) ** (1 / 252)
    return (fator_lft * fator_di - 1) / (fator_di - 1)


def _calcular_pu(
    vna: float | Decimal,
    cotacao: float | Decimal,
) -> Decimal:
    """Calcula o preço unitário da LFT a partir do VNA e da cotação."""
    if any_is_empty(vna, cotacao):
        return Decimal("NaN")
    vna_decimal = truncar_decimal(vna, 6)
    cotacao_decimal = truncar_decimal(cotacao, 4)
    return truncar_decimal(vna_decimal * cotacao_decimal / BASE_COTACAO, 6)


def pu(
    vna: float | Decimal,
    cotacao: float | Decimal,
) -> Decimal:
    """
    Calcula o PU da LFT pela metodologia da STN para leilões primários.

    Args:
        vna: Valor nominal atualizado (VNA).
        cotacao: Cotação da LFT em base 100.

    Returns:
        Decimal: Preço da LFT truncado em 6 casas decimais.

    References:
        - Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos
          Públicos Federais Ofertados nos Leilões Primários.
          https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

    Examples:
        >>> from pyield import lft
        >>> lft.pu(15785.324502, 99.9291)
        Decimal('15774.132706')
        >>> lft.pu(3451.2153459, 100.11589)
        Decimal('3455.211852')
    """
    return _calcular_pu(vna, cotacao)
