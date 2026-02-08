import datetime as dt

import polars as pl

from . import historical_b3 as hb3
from . import historical_bmf as hmf

DATA_LIMITE_API_ANTIGA = dt.date(2025, 12, 12)


def _buscar_df_historico(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca o histórico do contrato futuro para a data de referência."""
    if data > DATA_LIMITE_API_ANTIGA:
        return hb3._buscar_df_historico_b3(data, codigo_contrato)
    # Tenta buscar do serviço histórico antigo
    return hmf._buscar_df_historico(data, codigo_contrato)
