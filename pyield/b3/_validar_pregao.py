import datetime as dt
import logging

from pyield import bday, clock

registro = logging.getLogger(__name__)


def data_negociacao_valida(data_negociacao: dt.date) -> bool:
    """Valida se a data de referência é utilizável para consulta.

    Critérios:
    - Deve ser um dia útil brasileiro.
    - Não pode estar no futuro (maior que a data corrente no Brasil).

    Retorna True se válida, False caso contrário (e loga um aviso).
    """
    if data_negociacao > clock.today():
        registro.warning(f"A data informada {data_negociacao} está no futuro.")
        return False
    if not bday.is_business_day(data_negociacao):
        registro.warning(f"A data informada {data_negociacao} não é dia útil.")
        return False

    # Não tem pregão na véspera de Natal e Ano Novo
    datas_fechadas_especiais = {  # Datas especiais
        dt.date(data_negociacao.year, 12, 24),  # Véspera de Natal
        dt.date(data_negociacao.year, 12, 31),  # Véspera de Ano Novo
    }
    if data_negociacao in datas_fechadas_especiais:
        registro.warning(
            f"Não há pregão na véspera de Natal e de Ano Novo: {data_negociacao}"
        )
        return False

    return True
