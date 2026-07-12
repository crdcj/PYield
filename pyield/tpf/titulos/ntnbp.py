"""Precificação de NTN-B Principal pelas regras do Tesouro Direto."""

import polars as pl

from pyield import du, interpolador
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf.titulos import _utils as utils
from pyield.tpf.titulos import _zero_td

taxas_zero = _zero_td.taxas_zero


def cotacao(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa_tir: float,
) -> float:
    """
    Calcula a cotação da NTN-B Principal em base 1 pelo método do Tesouro Direto.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa_tir (float): Taxa interna de retorno anualizada do título.

    Returns:
        float: Cotação em base 1, truncada em 6 casas decimais.

    Examples:
        >>> from pyield import ntnbp
        >>> ntnbp.cotacao("02-12-2025", "15-05-2029", 0.0777)
        0.77463
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa_tir):
        return float("nan")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    anos_uteis = utils.truncar(dias_uteis / 252, 14)
    return utils.truncar(1 / (1 + taxa_tir) ** anos_uteis, 6)


def pu(vna: float, cotacao: float) -> float:
    """
    Calcula o preço (PU) da NTN-B Principal.

    Args:
        vna (float): Valor nominal atualizado (VNA).
        cotacao (float): Cotação da NTN-B Principal em base 1.

    Returns:
        float: Preço da NTN-B Principal truncado em 6 casas decimais.

    Examples:
        >>> from pyield import ntnbp
        >>> cot = ntnbp.cotacao("02-12-2025", "15-05-2029", 0.0777)
        >>> ntnbp.pu(4567.033825, cot)
        3537.761411
    """
    if any_is_empty(vna, cotacao):
        return float("nan")
    return utils.truncar(vna * cotacao, 6)


def _normalizar_curva_zero(curva_zero: pl.DataFrame) -> pl.DataFrame:
    """Valida e normaliza a curva zero usada na precificação."""
    colunas_necessarias = {"dias_uteis", "taxa_zero"}
    if not colunas_necessarias.issubset(curva_zero.columns):
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


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    curva_zero: pl.DataFrame,
) -> float:
    """
    Obtém a TIR de mercado da NTN-B Principal pelo método do Tesouro Direto.

    A taxa zero correspondente ao vencimento é interpolada por flat-forward e
    arredondada em quatro casas decimais. Como a NTN-B Principal possui um único
    fluxo no vencimento, essa taxa zero também é a TIR do título.

    A curva pode ser produzida por :func:`pyield.ntnbp.taxas_zero`. Para
    cálculos em lote, ela deve ser construída uma única vez e reutilizada entre
    os títulos.

    Args:
        data_liquidacao: Data de liquidação usada na construção da curva.
        data_vencimento: Data de vencimento da NTN-B Principal.
        curva_zero: DataFrame com as colunas ``dias_uteis`` e ``taxa_zero``.

    Returns:
        float: TIR de mercado anualizada, arredondada em quatro casas decimais.
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return float("nan")

    curva = _normalizar_curva_zero(curva_zero)
    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    taxa_zero = interpolador.Interpolador(
        curva["dias_uteis"],
        curva["taxa_zero"],
        metodo="flat_forward",
    ).interpolar(dias_uteis)
    return round(taxa_zero, 4)


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa_tir: float,
    pu: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B Principal em R$.

    Representa a variação do PU informado para um aumento de 1 bp (0,01%) na
    taxa.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa_tir (float): Taxa interna de retorno anualizada do título.
        pu (float): PU usado como base para o cálculo.

    Returns:
        float: DV01 (Dollar Value of 01), variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnbp as bp
        >>> cot = bp.cotacao("02-12-2025", "15-05-2029", 0.0777)
        >>> pu = bp.pu(4567.033825, cot)
        >>> bp.dv01("02-12-2025", "15-05-2029", 0.0777, pu)
        1.120055806382451
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa_tir, pu):
        return float("nan")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    anos_uteis = utils.truncar(dias_uteis / 252, 14)
    fator_preco = (1 + taxa_tir) ** anos_uteis
    fator_preco_1bp = (1 + taxa_tir + 0.0001) ** anos_uteis
    return pu * (1 - fator_preco / fator_preco_1bp)
