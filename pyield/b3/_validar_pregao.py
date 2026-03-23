import datetime as dt

from pyield import bday, clock


def data_negociacao_valida(data_negociacao: dt.date) -> bool:
    """Valida se a data de referência é utilizável para consulta.

    Critérios:
    - Deve ser um dia útil brasileiro.
    - Não pode estar no futuro (maior que a data corrente no Brasil).

    Retorna True se válida, False caso contrário.
    """
    if data_negociacao > clock.today():
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
