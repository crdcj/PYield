import datetime as dt

from pyield import bday, relogio

# Pregão abre às 9:00, porém os dados intradia têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
HORA_INICIO_INTRADIA = dt.time(9, 16)


def data_negociacao_valida(data_negociacao: dt.date) -> bool:
    """Valida se a data de referência é utilizável para consulta.

    Critérios:
    - Deve ser um dia útil brasileiro.
    - Não pode estar no futuro (maior que a data corrente no Brasil).

    Retorna True se válida, False caso contrário.
    """
    if data_negociacao > relogio.hoje():
        return False
    if not bday.is_business_day(data_negociacao):
        return False

    # Não tem pregão na véspera de Natal e Ano Novo
    datas_fechadas_especiais = {
        dt.date(data_negociacao.year, 12, 24),
        dt.date(data_negociacao.year, 12, 31),
    }
    if data_negociacao in datas_fechadas_especiais:
        return False

    return True


def intradia_disponivel() -> bool:
    """Verifica se dados intradia estão disponíveis agora.

    Critérios:
    - Hoje deve ser um dia de pregão válido.
    - O horário atual deve ser após o início dos dados intradia (9:16h).
    """
    if not data_negociacao_valida(relogio.hoje()):
        return False
    return relogio.agora().time() >= HORA_INICIO_INTRADIA
