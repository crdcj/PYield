import polars as pl

import pyield._internal.converters as cv
from pyield import du
from pyield._internal.types import DateLike, any_is_empty
from pyield.bc.vna import vna_bcb
from pyield.tn import utils


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
    df = utils.obter_tpf(data, "LFT")
    if df.is_empty():
        return df

    data_ref = cv.converter_datas(data)

    df = df.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
    )

    df = df.with_columns(
        prazo_medio=pl.col("dias_uteis") / 252,
    )
    df = utils.adicionar_taxa_di(df, data_ref)

    df = df.with_columns(
        rentabilidade=pl.struct("taxa_indicativa", "taxa_di").map_elements(
            lambda s: rentabilidade(s["taxa_indicativa"], s["taxa_di"]),
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


def vna(data: DateLike) -> float:
    """Busca o Valor Nominal Atualizado (VNA) da LFT.

    Fonte: Banco Central do Brasil, arquivo diário do SELIC.

    Args:
        data: Data de referência.

    Returns:
        Valor do VNA da LFT. Retorna ``nan`` se a entrada for nula ou vazia.

    Raises:
        ValueError: Se os valores VNA extraídos da fonte forem divergentes.
        requests.exceptions.HTTPError: Se a requisição ao BCB falhar.

    Examples:
        >>> from pyield import lft
        >>> lft.vna("31-05-2024")
        14903.01148
    """
    return vna_bcb(data)


def cotacao(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    """
    Calcula a cotação de uma LFT pelas regras da ANBIMA.

    Args:
        data_liquidacao: Data de liquidação do título.
        data_vencimento: Data de vencimento do título.
        taxa: Taxa anualizada do título.

    Returns:
        float: Cotação do título.

    Examples:
        Calcula a cotação de uma LFT com taxa de 0,02:
        >>> from pyield import lft
        >>> lft.cotacao(
        ...     data_liquidacao="24-07-2024",
        ...     data_vencimento="01-09-2030",
        ...     taxa=0.001717,  # 0.1717%
        ... )
        98.9645

        Entradas nulas retornam float('nan'):
        >>> lft.cotacao(
        ...     data_liquidacao=None, data_vencimento="01-09-2030", taxa=0.001717
        ... )
        nan
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")
    # Número de dias úteis entre liquidação (inclusivo) e vencimento (exclusivo)
    dias_uteis = du.contar(data_liquidacao, data_vencimento)

    # Número de períodos truncado conforme regras da ANBIMA
    anos_truncados = utils.truncar(dias_uteis / 252, 14)

    fator_desconto = 1 / (1 + taxa) ** anos_truncados

    return utils.truncar(100 * fator_desconto, 4)


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    vna: float,
    pu: float,
) -> float:
    """
    Calcula a taxa implícita de uma LFT a partir do preço (PU).

    A função inverte numericamente a cadeia ``pu(vna, cotacao(...))``,
    encontrando a taxa que zera a diferença entre o preço calculado e o
    informado.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        vna (float): Valor nominal atualizado (VNA).
        pu (float): Preço unitário (PU) do título.

    Returns:
        float: Taxa implícita em formato decimal. Retorna NaN em
            caso de erro.

    Examples:
        >>> from pyield import lft
        >>> lft.taxa("24-07-2024", "01-09-2030", 15785.324502, 15621.867466)
        0.001717
        >>> lft.taxa("24-07-2024", "01-03-2025", 15785.324502, 15774.132706)
        0.00116
    """
    if any_is_empty(data_liquidacao, data_vencimento, vna, pu):
        return float("nan")

    if pu <= 0:
        return float("nan")

    def diferenca_preco(taxa: float) -> float:
        return _calcular_pu(vna, cotacao(data_liquidacao, data_vencimento, taxa)) - pu

    taxa_encontrada = utils.encontrar_raiz(diferenca_preco)
    return round(taxa_encontrada, 6)


def rentabilidade(taxa_lft: float, taxa_di: float) -> float:
    """
    Calcula a rentabilidade da LFT sobre a taxa de DI Futuro.

    Args:
        taxa_lft: Taxa anualizada da LFT sobre a Selic.
        taxa_di: Taxa DI Futuro anualizada (interpolada para o mesmo
            vencimento da LFT).

    Returns:
        float: Rentabilidade da LFT sobre o DI.

    Examples:
        Calcula a rentabilidade de uma LFT em 28/04/2025:
        >>> from pyield import lft
        >>> taxa_lft = 0.001124  # 0.1124%
        >>> taxa_di = 0.13967670224373396  # 13.967670224373396%
        >>> lft.rentabilidade(taxa_lft, taxa_di)
        1.008594331960501
    """
    if any_is_empty(taxa_lft, taxa_di):
        return float("nan")
    # Taxa diária
    fator_lft = (taxa_lft + 1) ** (1 / 252)
    fator_di = (taxa_di + 1) ** (1 / 252)
    return (fator_lft * fator_di - 1) / (fator_di - 1)


def _calcular_pu(
    vna: float,
    cotacao: float,
) -> float:
    """Calcula o preço unitário da LFT a partir do VNA e da cotação."""
    if any_is_empty(vna, cotacao):
        return float("nan")
    return utils.truncar(vna * cotacao / 100, 6)


def pu(
    vna: float,
    cotacao: float,
) -> float:
    """
    Calcula o preço (PU) da LFT pelas regras da Anbima.

    Args:
        vna (float): Valor nominal atualizado (VNA).
        cotacao (float): Cotação da LFT em base 100.
    Returns:
        float: Preço da LFT truncado em 6 casas decimais.

    References:
         - SEI Proccess 17944.005214/2024-09

    Examples:
        >>> from pyield import lft
        >>> lft.pu(15785.324502, 99.9291)
        15774.132706
    """
    return _calcular_pu(vna, cotacao)
