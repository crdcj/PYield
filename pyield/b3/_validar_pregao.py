import datetime as dt

from pyield import du, relogio
from pyield._internal.converters import converter_datas
from pyield._internal.types import DateLike

# Pregão abre às 9:00, porém os dados intradia têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
HORA_INICIO_INTRADIA = dt.time(9, 16)

# Fechamentos recorrentes da B3 que não são feriados nacionais.
_FECHAMENTOS_B3 = frozenset({(12, 24), (12, 31)})


def eh_pregao(data: DateLike) -> bool:
    """Verifica se há pregão na B3 na data informada.

    Há pregão em todo dia útil brasileiro, exceto nas vésperas de Natal (24/12)
    e de Ano Novo (31/12), quando a B3 não abre.

    Args:
        data: Data a verificar.

    Returns:
        ``True`` se houver pregão na data.

    Notes:
        - Usa os feriados nacionais de ``yd.du`` com ``calendario="auto"``.
        - Fechamentos excepcionais ou históricos não recorrentes, como feriados
          municipais de São Paulo em anos anteriores, não são modelados.
        - Strings de data aceitas: ``DD-MM-YYYY``, ``DD/MM/YYYY`` e ``YYYY-MM-DD``.

    Examples:
        >>> yd.b3.eh_pregao("23-12-2025")
        True

        Dia útil sem pregão:
        >>> yd.b3.eh_pregao("24-12-2025")
        False
    """
    data = converter_datas(data)
    return bool(du.eh_dia_util(data)) and (data.month, data.day) not in _FECHAMENTOS_B3


def _pregao_vizinho(data: DateLike, passo: int) -> dt.date:
    # Anda dia a dia: `du.deslocar` ajusta datas sem pregão antes de deslocar.
    data = converter_datas(data) + dt.timedelta(days=passo)
    while not eh_pregao(data):
        data += dt.timedelta(days=passo)
    return data


def pregao_anterior(data: DateLike) -> dt.date:
    """Retorna o último pregão da B3 estritamente anterior à data.

    Args:
        data: Data de referência, com ou sem pregão.

    Returns:
        Data do pregão anterior.

    Examples:
        >>> yd.b3.pregao_anterior("02-01-2026")
        datetime.date(2025, 12, 30)
    """
    return _pregao_vizinho(data, -1)


def proximo_pregao(data: DateLike) -> dt.date:
    """Retorna o primeiro pregão da B3 estritamente posterior à data.

    Args:
        data: Data de referência, com ou sem pregão.

    Returns:
        Data do próximo pregão.

    Examples:
        Sábado seguido do feriado de Finados:
        >>> yd.b3.proximo_pregao("31-10-2026")
        datetime.date(2026, 11, 3)
    """
    return _pregao_vizinho(data, 1)


def ultimo_pregao(data: DateLike | None = None) -> dt.date:
    """Retorna o último pregão da B3 na data ou antes dela.

    Args:
        data: Data de referência. Se None, usa a data atual no Brasil.

    Returns:
        A própria data, se houver pregão; caso contrário, o pregão anterior.

    Examples:
        >>> yd.b3.ultimo_pregao("30-12-2025")
        datetime.date(2025, 12, 30)
        >>> yd.b3.ultimo_pregao("31-12-2025")
        datetime.date(2025, 12, 30)
    """
    data = relogio.hoje() if data is None else converter_datas(data)
    return data if eh_pregao(data) else pregao_anterior(data)


def data_negociacao_valida(data_negociacao: dt.date) -> bool:
    """Valida se a data de referência é utilizável para consulta.

    Critérios:
    - Deve haver pregão na B3 na data.
    - Não pode estar no futuro (maior que a data corrente no Brasil).

    Retorna True se válida, False caso contrário.
    """
    return data_negociacao <= relogio.hoje() and eh_pregao(data_negociacao)


def intradia_disponivel() -> bool:
    """Verifica se dados intradia estão disponíveis agora.

    Critérios:
    - Hoje deve ser um dia de pregão válido.
    - O horário atual deve ser após o início dos dados intradia (9:16h).
    """
    if not data_negociacao_valida(relogio.hoje()):
        return False
    return relogio.agora().time() >= HORA_INICIO_INTRADIA
