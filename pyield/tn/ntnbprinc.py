from pyield import bday
from pyield._internal.types import DateLike, any_is_empty
from pyield.tn import utils


def pu(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
    vna: float,
) -> float:
    """
    Calcula o preço (PU) da NTN-B Principal pelas regras do Tesouro Nacional.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float): Taxa de desconto (YTM) do título.
        vna (float): Valor nominal atualizado (VNA).

    Returns:
        float: Preço (PU) da NTN-B Principal conforme as regras do Tesouro Nacional.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnbprinc
        >>> ntnbprinc.pu("02-12-2025", "15-05-2029", 0.0777, 4567.033825)
        3537.763157
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa, vna):
        return float("nan")

    # Calcula dias úteis entre liquidação e vencimento
    dias_uteis = bday.count(data_liquidacao, data_vencimento)

    # Calcula anos úteis truncados conforme ANBIMA
    anos_uteis = utils.truncar(dias_uteis / 252, 14)

    fator_desconto = (1 + taxa) ** anos_uteis

    # Trunca o preço em 6 casas conforme ANBIMA
    return utils.truncar(vna / fator_desconto, 6)


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
    vna: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B Principal em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float): Taxa de desconto (YTM) do título.
        vna (float): Valor nominal atualizado (VNA).

    Returns:
        float: DV01 (Dollar Value of 01), variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnbprinc as bp
        >>> bp.dv01("02-12-2025", "15-05-2029", 0.0777, 4567.033825)
        1.1200559999997495
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa, vna):
        return float("nan")

    preco_1 = pu(data_liquidacao, data_vencimento, taxa, vna)
    preco_2 = pu(data_liquidacao, data_vencimento, taxa + 0.0001, vna)
    return preco_1 - preco_2
