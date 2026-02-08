import datetime as dt

import polars as pl

from . import historical_b3 as hb3
from . import historical_bmf as hmf

LAST_SUPPORTED_DATE_OLD_API = dt.date(2025, 12, 12)


def _buscar_df_historico(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca o histórico do contrato futuro para a data de referência."""
    if data > LAST_SUPPORTED_DATE_OLD_API:
        return hb3._fetch_historical_df(data, codigo_contrato)
    else:
        # Tenta buscar do serviço histórico antigo
        return hmf._buscar_df_historico(data, codigo_contrato)
