import datetime as dt
import logging

import polars as pl

from pyield import bday, clock

registro = logging.getLogger(__name__)


def expr_dv01(
    coluna_dias_uteis: str,
    coluna_taxa: str,
    coluna_preco: str,
) -> pl.Expr:
    """Retorna a expressão Polars para cálculo de DV01.

    Fórmula:
    DV01 = (Duration / (1 + taxa)) * preço * 0,0001

    Onde:
    - Duration = dias_uteis / 252
    - taxa deve estar em formato decimal (ex.: 0.145)
    - preço é o PU do contrato.
    """
    duracao = pl.col(coluna_dias_uteis) / 252
    duracao_modificada = duracao / (1 + pl.col(coluna_taxa))
    return 0.0001 * duracao_modificada * pl.col(coluna_preco)


_MAPA_MESES: dict[str, int] = {
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
_MAPA_MESES_STR: dict[str, str] = {k: str(v) for k, v in _MAPA_MESES.items()}


def _corrigir_vencimento_cpm(df: pl.DataFrame, coluna_ticker: str) -> pl.DataFrame:
    """
    Replace ExpirationDate for CPM contracts using the COPOM calendar.

    CPM options expire on the first business day AFTER the COPOM meeting end date,
    not on the first business day of the meeting month (which is what the generic
    adicionar_vencimento computes).  The correct date is fetched from the COPOM
    calendar (pyield.bc.copom.calendar()) using meeting month+year as the join key.

    If the calendar has no entry for a given meeting (e.g. a very distant future
    meeting not yet in the hardcoded list), the original ExpirationDate is kept.
    """
    from pyield.bc import copom  # noqa: PLC0415  (deferred to avoid circular import)

    cal = copom.calendar().select(
        _meeting_month=pl.col("EndDate").dt.month().cast(pl.Int32),
        _meeting_year=pl.col("EndDate").dt.year().cast(pl.Int32),
        _calendar_expiry=pl.col("ExpiryDate"),
    )

    df = df.with_columns(
        _meeting_month=(
            pl.col(coluna_ticker)
            .str.slice(3, 1)
            .replace(_MAPA_MESES_STR)
            .cast(pl.Int32, strict=False)
        ),
        _meeting_year=(
            pl.col(coluna_ticker)
            .str.slice(4, 2)
            .cast(pl.Int32, strict=False)
            .add(2000)
        ),
    )

    df = df.join(cal, on=["_meeting_month", "_meeting_year"], how="left")

    df = df.with_columns(
        ExpirationDate=pl.when(pl.col("_calendar_expiry").is_not_null())
        .then(pl.col("_calendar_expiry"))
        .otherwise(pl.col("ExpirationDate"))
    )

    return df.drop("_meeting_month", "_meeting_year", "_calendar_expiry")


def adicionar_vencimento(
    df: pl.DataFrame, codigo_contrato: str, coluna_ticker: str
) -> pl.DataFrame:
    """
    Recebe um DataFrame Polars e ADICIONA a coluna 'ExpirationDate'.

    - Pega a coluna 'coluna_ticker'.
    - Extrai o código de vencimento.
    - Converte para a data "bruta", sem ajuste de feriado.
    - Garante que a data de vencimento é um dia útil.
    - Retorna o DataFrame com a nova coluna ExpirationDate.

    For CPM contracts specifically, the ExpirationDate is corrected by joining
    against the COPOM calendar (first bday after meeting end), since the generic
    formula (first bday of month) is wrong for CPM.
    """
    dia_vencimento = 15 if "DAP" in codigo_contrato else 1
    df = df.with_columns(
        pl.date(
            # Ano: posição 4-5 (2 dígitos) -> Int -> Soma 2000
            # Funciona para tickers de 6 chars (DI1F25) e 13 chars (CPMF25C100750)
            year=pl.col(coluna_ticker).str.slice(4, 2).cast(pl.Int32, strict=False)
            + 2000,
            # Mês: posição 3 (1 char = código do mês) -> mapeia para Int
            month=pl.col(coluna_ticker)
            .str.slice(3, 1)
            .replace_strict(_MAPA_MESES, default=None, return_dtype=pl.Int8),
            day=dia_vencimento,
        ).alias("ExpirationDate")
    )
    # Garante que a data de vencimento é um dia útil
    df = df.with_columns(ExpirationDate=bday.offset_expr("ExpirationDate", 0))

    if codigo_contrato == "CPM":
        df = _corrigir_vencimento_cpm(df, coluna_ticker)

    return df


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
            "Não há pregão na véspera de Natal e de Ano Novo: "
            f"{data_negociacao}"
        )
        return False

    return True
