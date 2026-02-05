import datetime as dt
from zoneinfo import ZoneInfo

# Fuso horário do Brasil (São Paulo), usado para mercados e B3
BR_TZ = ZoneInfo("America/Sao_Paulo")


def now() -> dt.datetime:
    """Retorna o datetime atual no fuso horário do Brasil.

    O fuso horário usado é America/Sao_Paulo, que é o padrão para o mercado
    financeiro brasileiro e a B3.

    Returns:
        Datetime atual no fuso horário do Brasil.

    Examples:
        >>> import datetime as dt
        >>> from pyield import clock
        >>> resultado = clock.now()
        >>> isinstance(resultado, dt.datetime)
        True
        >>> str(resultado.tzinfo) == "America/Sao_Paulo"
        True
    """
    return dt.datetime.now(BR_TZ)


def today() -> dt.date:
    """Retorna a data de hoje no fuso horário do Brasil.

    A data é determinada com base no fuso horário America/Sao_Paulo, que é o
    padrão para o mercado financeiro brasileiro.

    Returns:
        Data de hoje no fuso horário do Brasil.

    Examples:
        >>> import datetime as dt
        >>> from pyield import clock
        >>> resultado = clock.today()
        >>> isinstance(resultado, dt.date)
        True
    """
    return now().date()
