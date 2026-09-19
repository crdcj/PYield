"""Precificação de NTN-B Principal (Tesouro IPCA+ sem cupons).

Convenções de cálculo:
    - Fluxo único no vencimento, atualizado pelo IPCA via VNA.
    - Prazo de desconto: dias úteis / 252, truncado a 14 casas.
    - Cotação: base 100, truncada a 4 casas.
    - VNA de entrada e PU retornado: truncados a 6 casas.
    - Taxa da curva: float interpolado por flat-forward, sem arredondamento.

O exemplo de precificação do Tesouro Direto (páginas 2 a 4) usa cotação com
4 casas, embora o texto da página 4 mencione 2. O PU da biblioteca mantém
6 casas; o preço em centavos do documento exige truncamento adicional.
O documento parte de uma taxa pactuada e não especifica a regra de
arredondamento da taxa obtida por curva zero.

References:
    - Tesouro Direto — Cálculo da Rentabilidade: Tesouro IPCA+.
      <https://www.tesourodireto.com.br/documents/d/guest/tesouro_ipca_juros_semestrais>
"""

from decimal import Decimal

import polars as pl

from pyield import du, interpolador
from pyield._internal.numbers import truncar_decimal
from pyield._internal.types import DateLike, any_is_empty

from . import _utils


def cotacao(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa_tir: float | Decimal | str,
) -> Decimal:
    """
    Calcula a cotação da NTN-B Principal em base 100 descontando o principal pela TIR.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        taxa_tir: Taxa interna de retorno anualizada do título.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
            A taxa de entrada não é truncada antes do cálculo.

    Returns:
        Decimal: Cotação em base 100, truncada em 4 casas decimais. Retorna
            ``Decimal("NaN")`` se a liquidação for igual ou posterior ao vencimento.

    Examples:
        >>> from pyield import ntnbp
        >>> ntnbp.cotacao("02-12-2025", "15-05-2029", "7.77%")
        Decimal('77.4630')

        Exemplo do Tesouro Direto, páginas 2 a 4: liquidação em 03/01/2012,
        vencimento em 15/05/2015, prazo de 846 dias úteis e taxa de 5,17% a.a.

        >>> ntnbp.cotacao("03-01-2012", "15-05-2015", "5.17%")
        Decimal('84.4317')

    References:
        - Tesouro Direto — Tesouro IPCA+, páginas 2 a 4.
          <https://www.tesourodireto.com.br/documents/d/guest/tesouro_ipca_juros_semestrais>
    """
    if isinstance(taxa_tir, str):
        taxa_tir = float(_utils.converter_taxa(taxa_tir))
    if any_is_empty(data_liquidacao, data_vencimento, taxa_tir):
        return Decimal("NaN")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return Decimal("NaN")
    anos_uteis = _utils.truncar(dias_uteis / 252, 14)
    cotacao_percentual = 100 / (1 + float(taxa_tir)) ** anos_uteis
    return truncar_decimal(cotacao_percentual, 4)


def pu(vna: float | Decimal, cotacao: float | Decimal) -> Decimal:
    """
    Calcula o preço (PU) da NTN-B Principal.

    Args:
        vna: Valor nominal atualizado (VNA), truncado em seis casas decimais
            antes do cálculo, sem arredondar.
        cotacao: Cotação da NTN-B Principal em base 100.
            Truncada em quatro casas decimais antes do cálculo, sem arredondar.

    Returns:
        Decimal: Preço da NTN-B Principal truncado em 6 casas decimais.

    Examples:
        >>> from pyield import ntnbp
        >>> cot = ntnbp.cotacao("02-12-2025", "15-05-2029", "7.77%")
        >>> ntnbp.pu(4567.033825, cot)
        Decimal('3537.761411')

        Exemplo do Tesouro Direto, página 4, com o VNA projetado publicado.
        O retorno preserva seis casas; truncado em centavos, corresponde
        ao preço de R$ 1.776,77 apresentado no documento.

        >>> cot = ntnbp.cotacao("03-01-2012", "15-05-2015", "5.17%")
        >>> ntnbp.pu(Decimal("2104.390122"), cot)
        Decimal('1776.772354')

    References:
        - Tesouro Direto — Tesouro IPCA+, página 4.
          <https://www.tesourodireto.com.br/documents/d/guest/tesouro_ipca_juros_semestrais>
    """
    if any_is_empty(vna, cotacao):
        return Decimal("NaN")
    vna_decimal = truncar_decimal(vna, 6)
    cotacao_decimal = truncar_decimal(cotacao, 4)
    return truncar_decimal(vna_decimal * cotacao_decimal / 100, 6)


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
    Obtém a TIR da NTN-B Principal a partir da curva zero informada.

    A taxa zero correspondente ao vencimento é interpolada por flat-forward,
    sem arredondamento. Como a NTN-B Principal possui um único
    fluxo no vencimento, essa taxa zero também é a TIR do título.

    Arredondamentos comerciais e spreads de compra ou venda ficam a cargo
    do consumidor.

    A curva pode ser produzida por :func:`pyield.ntnb.taxas_zero`. Para
    cálculos em lote, ela deve ser construída uma única vez e reutilizada entre
    os títulos.

    Args:
        data_liquidacao: Data de liquidação usada na construção da curva.
        data_vencimento: Data de vencimento da NTN-B Principal.
        curva_zero: DataFrame com as colunas ``dias_uteis`` e ``taxa_zero``.

    Returns:
        float: TIR anualizada em formato decimal, sem arredondamento.
            Retorna ``NaN`` se a liquidação for igual ou posterior ao vencimento.
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return float("nan")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return float("nan")

    curva = _normalizar_curva_zero(curva_zero)
    return interpolador.Interpolador(
        curva["dias_uteis"],
        curva["taxa_zero"],
        metodo="flat_forward",
    ).interpolar(dias_uteis)


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa_tir: float | str,
    pu: float | Decimal,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B Principal em R$.

    Representa a variação do PU informado para um aumento de 1 bp (0,01%) na
    taxa.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa_tir (float): Taxa interna de retorno anualizada do título.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
        pu: PU usado como base para o cálculo.

    Returns:
        float: DV01 (Dollar Value of 01), variação de preço para 1 bp. Retorna
            ``NaN`` se a liquidação for igual ou posterior ao vencimento.

    Examples:
        >>> from pyield import ntnbp as bp
        >>> cot = bp.cotacao("02-12-2025", "15-05-2029", "7.77%")
        >>> pu = bp.pu(4567.033825, cot)
        >>> bp.dv01("02-12-2025", "15-05-2029", "7.77%", pu)
        1.120055806382451
    """
    if isinstance(taxa_tir, str):
        taxa_tir = float(_utils.converter_taxa(taxa_tir))
    if any_is_empty(data_liquidacao, data_vencimento, taxa_tir, pu):
        return float("nan")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return float("nan")
    anos_uteis = _utils.truncar(dias_uteis / 252, 14)
    fator_preco = (1 + taxa_tir) ** anos_uteis
    fator_preco_1bp = (1 + taxa_tir + 0.0001) ** anos_uteis
    return float(pu) * (1 - fator_preco / fator_preco_1bp)
