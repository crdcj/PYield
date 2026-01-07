import datetime as dt
import logging

import polars as pl

from pyield import bday, clock

logger = logging.getLogger(__name__)


def add_expiration_date(
    df: pl.DataFrame, ticker_column: str, expiration_day: int = 1
) -> pl.DataFrame:
    """
    Recebe um DataFrame Polars e ADICIONA a coluna 'ExpirationDate'.

    - Pega a coluna 'ticker_column'.
    - Extrai o código de vencimento.
    - Converte para a data "bruta", sem ajuste de feriado.
    - Garante que a data de vencimento é um dia útil.
    - Retorna o DataFrame com a nova coluna ExpirationDate.
    """

    month_map = {
        "F": 1,
        "G": 2,
        "H": 3,
        "J": 4,
        "K": 5,
        "M": 6,
        "N": 7,
        "Q": 8,
        "U": 9,
        "V": 10,
        "X": 11,
        "Z": 12,
    }
    df = df.with_columns(
        pl.date(
            # Ano: Pega os 2 últimos dígitos -> Int -> Soma 2000
            year=pl.col(ticker_column).str.slice(-2).cast(pl.Int32, strict=False)
            + 2000,
            # Mês: Pega 1ª letra -> Mapeia -> Int
            month=pl.col(ticker_column)
            .str.slice(-3, 1)
            .replace_strict(month_map, default=None, return_dtype=pl.Int8),
            day=expiration_day,
        ).alias("ExpirationDate")
    )
    # Garantee que a data de vencimento é um dia útil
    adj_dates = bday.offset(dates=df["ExpirationDate"], offset=0)
    df = df.with_columns(ExpirationDate=adj_dates)
    return df


def is_trade_date_valid(trade_date: dt.date) -> bool:
    """Valida se a data de referência é utilizável para consulta.

    Critérios:
    - Deve ser um dia útil brasileiro.
    - Não pode estar no futuro (maior que a data corrente no Brasil).

    Retorna True se válida, False caso contrário (e loga um aviso).
    """
    if trade_date > clock.today():
        logger.warning(f"The provided date {trade_date} is in the future.")
        return False
    if not bday.is_business_day(trade_date):
        logger.warning(f"The provided date {trade_date} is not a business day.")
        return False

    # Não tem pregão na véspera de Natal e Ano Novo
    special_closed_dates = {  # Datas especiais
        dt.date(trade_date.year, 12, 24),  # Véspera de Natal
        dt.date(trade_date.year, 12, 31),  # Véspera de Ano Novo
    }
    if trade_date in special_closed_dates:
        logger.warning(
            "There is no trading session before Christmas and New Year's Eve: "
            f"{trade_date}"
        )
        return False

    return True
