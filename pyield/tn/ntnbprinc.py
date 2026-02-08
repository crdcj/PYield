from pyield import bday
from pyield.tn import tools
from pyield.types import DateLike, any_is_empty


def price(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    face_value: float,
) -> float:
    """
    Calcula o preço da NTN-B Principal pelas regras da ANBIMA.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto (YTM) do título.
        face_value (float): Valor nominal atualizado (VNA).

    Returns:
        float: Preço da NTN-B Principal conforme ANBIMA.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnbprinc
        >>> ntnbprinc.price("02-12-2025", "15-05-2029", 0.0777, 4567.033825)
        3537.763157
    """
    if any_is_empty(settlement, maturity, rate, face_value):
        return float("nan")

    # Calcula dias úteis entre liquidação e vencimento
    dias_uteis = bday.count(settlement, maturity)

    # Calcula anos úteis truncados conforme ANBIMA
    anos_uteis = tools.truncate(dias_uteis / 252, 14)

    fator_desconto = (1 + rate) ** anos_uteis

    # Trunca o preço em 6 casas conforme ANBIMA
    return tools.truncate(face_value / fator_desconto, 6)


def dv01(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    face_value: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B Principal em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto (YTM) do título.
        face_value (float): Valor nominal atualizado (VNA).

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnbprinc as bp
        >>> bp.dv01("02-12-2025", "15-05-2029", 0.0777, 4567.033825)
        1.1200559999997495
    """
    if any_is_empty(settlement, maturity, rate, face_value):
        return float("nan")

    preco_1 = price(settlement, maturity, rate, face_value)
    preco_2 = price(settlement, maturity, rate + 0.0001, face_value)
    return preco_1 - preco_2
