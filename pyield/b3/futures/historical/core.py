import datetime as dt

import polars as pl
from dateutil.relativedelta import relativedelta

from pyield import clock
from pyield.b3.price_report import fetch_price_report

from . import historical_b3 as hb3

JANELA_DADOS_RECENTES = relativedelta(months=1)


def buscar_df_historico(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca o histórico com priorização de fonte por recência da data.

    Regras:
    - Data até 1 mês atrás: busca direto no Price Report (SPR).
    - Data dentro da janela recente: tenta endpoint curto (`historical_b3`).
    - Se o endpoint curto vier vazio: faz fallback para Price Report (SPR).
    """
    data_limite_recente = clock.today() - JANELA_DADOS_RECENTES

    # Datas antigas não tentam endpoint de janela curta.
    if data <= data_limite_recente:
        try:
            return fetch_price_report(
                date=data, contract_code=codigo_contrato, source_type="SPR"
            )
        except Exception:
            return pl.DataFrame()

    # Datas recentes priorizam endpoint curto; fallback para price report.
    df_recente = hb3.buscar_df_historico(data, codigo_contrato)
    if not df_recente.is_empty():
        return df_recente
    try:
        return fetch_price_report(
            date=data, contract_code=codigo_contrato, source_type="SPR"
        )
    except Exception:
        return pl.DataFrame()
