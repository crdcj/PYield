from pyield import bday
from pyield.tn import tools
from pyield.types import DateLike, has_nullable_args


def price(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    face_value: float,
) -> float:
    """
    Calculate the NTN-B PRINCIPAL price using Anbima rules.

    Args:
        settlement (DateScalar): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (DateScalar): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the NTN-F.
        face_value (float): The face value of the bond (VNA).
    Returns:
        float: The NTN-B PRINCIPAL price using Anbima rules.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnbprinc
        >>> ntnbprinc.price("02-12-2025", "15-05-2029", 0.0777, 4567.033825)
        3537.763157
    """
    if has_nullable_args(settlement, maturity, rate, face_value):
        return float("nan")

    # Calculate the number of business days between settlement and cash flow dates
    bdays = bday.count(settlement, maturity)

    # Calculate the number of periods truncated as per Anbima rule
    byears = tools.truncate(bdays / 252, 14)

    discount_factor = (1 + rate) ** byears

    # Truncate the price to 6 decimal places as per Anbima rules
    return tools.truncate(face_value / discount_factor, 6)


def dv01(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    face_value: float,
) -> float:
    """
    Calculate the DV01 (Dollar Value of 01) for an LTN in R$.

    Represents the price change in R$ for a 1 basis point (0.01%) increase in yield.

    Args:
        settlement (DateScalar): The settlement date in 'DD-MM-YYYY' format
            or a pandas Timestamp.
        maturity (DateScalar): The maturity date in 'DD-MM-YYYY' format or
            a pandas Timestamp.
        rate (float): The discount rate used to calculate the present value of
            the cash flows, which is the yield to maturity (YTM) of the LTN.
        face_value (float): The face value of the bond (VNA).

    Returns:
        float: The DV01 value, representing the price change for a 1 basis point
            increase in yield.

    Examples:
        >>> from pyield import ntnbprinc as bp
        >>> bp.dv01("02-12-2025", "15-05-2029", 0.0777, 4567.033825)
        1.1200559999997495
    """
    if has_nullable_args(settlement, maturity, rate, face_value):
        return float("nan")

    price1 = price(settlement, maturity, rate, face_value)
    price2 = price(settlement, maturity, rate + 0.0001, face_value)
    return price1 - price2
