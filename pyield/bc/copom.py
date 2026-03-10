"""
copom — COPOM meeting calendar.

Past meetings sourced from the BCB public API (atas endpoint).
Future meetings for the current cycle are hardcoded from the
official BCB public note and must be updated each January.

BCB API field mapping
---------------------
    nroReuniao     → MeetingNumber  (sequential BCB number)
    dataReferencia → EndDate        (last day of the 2-day meeting)
    StartDate      derived as EndDate − 1 calendar day (always 2-day meetings)
"""

from __future__ import annotations

import datetime
import logging

import polars as pl
import requests

from pyield import bday, clock
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike

logger = logging.getLogger(__name__)

URL_ATAS = "https://www.bcb.gov.br/api/servico/sitebcb/copom/atas"

_SCHEMA_CALENDARIO = {
    "MeetingNumber": pl.Int32,
    "StartDate": pl.Date,
    "EndDate": pl.Date,
}

# ---------------------------------------------------------------------------
# Future meetings — update every January from the official BCB public note
# Source: https://www.bcb.gov.br/controleinflacao/calendarioreunioescopom
# ---------------------------------------------------------------------------
_FUTURE_MEETINGS_2026: list[tuple[datetime.date, datetime.date]] = [
    # (StartDate, EndDate)
    (datetime.date(2026, 1, 27), datetime.date(2026, 1, 28)),
    (datetime.date(2026, 3, 17), datetime.date(2026, 3, 18)),
    (datetime.date(2026, 4, 28), datetime.date(2026, 4, 29)),
    (datetime.date(2026, 6, 16), datetime.date(2026, 6, 17)),
    (datetime.date(2026, 8, 4), datetime.date(2026, 8, 5)),
    (datetime.date(2026, 9, 15), datetime.date(2026, 9, 16)),
    (datetime.date(2026, 11, 3), datetime.date(2026, 11, 4)),
    (datetime.date(2026, 12, 8), datetime.date(2026, 12, 9)),
]

# Combine all future meetings here.  When a new year's calendar is
# published, add _FUTURE_MEETINGS_{YEAR} and append it to this list.
_ALL_FUTURE_MEETINGS: list[tuple[datetime.date, datetime.date]] = _FUTURE_MEETINGS_2026


@retry_padrao
def _chamar_api_atas(quantidade: int = 500) -> list[dict]:
    """Fetch raw COPOM meeting list from the BCB atas API."""
    resposta = requests.get(URL_ATAS, params={"quantidade": quantidade}, timeout=10)
    resposta.raise_for_status()
    return resposta.json().get("conteudo", [])


def _fetch_past_meetings() -> pl.DataFrame:
    """
    Fetch historical COPOM meetings from the BCB atas API.

    Returns a DataFrame with columns: MeetingNumber, StartDate, EndDate.
    StartDate is derived as EndDate − 1 calendar day (COPOM always meets
    over 2 consecutive days; the exact start is embedded in the title
    field but the simple subtraction is consistent across all records).
    """
    try:
        dados = _chamar_api_atas()
    except Exception:
        logger.exception("Falha ao buscar reuniões do Copom na API do BCB.")
        return pl.DataFrame(schema=_SCHEMA_CALENDARIO)

    if not dados:
        logger.warning("API do BCB retornou lista vazia para atas do Copom.")
        return pl.DataFrame(schema=_SCHEMA_CALENDARIO)

    return (
        pl.from_dicts(dados)
        .select(
            MeetingNumber=pl.col("nroReuniao").cast(pl.Int32),
            EndDate=pl.col("dataReferencia").str.to_date("%Y-%m-%d"),
        )
        .with_columns(StartDate=(pl.col("EndDate") - pl.duration(days=1)))
        .select("MeetingNumber", "StartDate", "EndDate")
    )


def _build_future_meetings() -> pl.DataFrame:
    """
    Build a DataFrame of future meetings from _ALL_FUTURE_MEETINGS.

    MeetingNumber is null (BCB assigns the number after the meeting).
    Only meetings whose EndDate is strictly after today are included,
    so already-past entries in the hardcoded list are silently skipped.
    """
    hoje = clock.today()
    rows = [{"StartDate": s, "EndDate": e} for s, e in _ALL_FUTURE_MEETINGS if e > hoje]

    if not rows:
        return pl.DataFrame(schema=_SCHEMA_CALENDARIO)

    return (
        pl.from_dicts(rows)
        .with_columns(MeetingNumber=pl.lit(None, dtype=pl.Int32))
        .select("MeetingNumber", "StartDate", "EndDate")
    )


def calendar(
    start: DateLike | None = None,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """
    Return the full COPOM meeting calendar (past + future).

    Past meetings are fetched live from the BCB API.
    Future meetings come from the hardcoded annual constant.
    Duplicates between the two sources are removed by deduplication
    on EndDate, so there is no need to manually keep the lists in sync.

    Parameters
    ----------
    start, end : DateLike | None
        Optional inclusive date range filter applied to EndDate.
        None means no bound.

    Returns
    -------
    pl.DataFrame
        Columns:
            MeetingNumber : Int32   BCB sequential number (null for future)
            StartDate     : Date    first day of the 2-day meeting
            EndDate       : Date    last day of the 2-day meeting
            ExpiryDate    : Date    next Brazilian business day after EndDate
                                    (= B3 CPM contract settlement/expiry date)
        Rows are sorted by EndDate ascending.

    Notes
    -----
    ExpiryDate is computed with ``bday.offset_expr("EndDate", 1)``, using
    the Brazilian holiday calendar already embedded in ``pyield.bday``.

    Examples
    --------
    >>> import pyield as yd
    >>> cal = yd.bc.copom.calendar()
    >>> "ExpiryDate" in cal.columns
    True
    >>> cal["EndDate"].is_sorted()
    True
    """
    past = _fetch_past_meetings()
    future = _build_future_meetings()

    df = (
        pl.concat([past, future], how="diagonal")
        .unique(subset=["EndDate"])
        .sort("EndDate")
    )

    # ExpiryDate: next business day after the meeting ends.
    # Vectorized via offset_expr — consistent with the rest of the codebase.
    df = df.with_columns(ExpiryDate=bday.offset_expr("EndDate", 1))

    # Optional date-range filter on EndDate
    if start is not None:
        start_date = converter_datas(start)
        if start_date is not None:
            df = df.filter(pl.col("EndDate") >= start_date)
    if end is not None:
        end_date = converter_datas(end)
        if end_date is not None:
            df = df.filter(pl.col("EndDate") <= end_date)

    return df


def next_meeting(reference: DateLike | None = None) -> pl.DataFrame:
    """
    Return the single next COPOM meeting on or after ``reference``.

    If ``reference`` is None, today's date (Brazil timezone) is used.
    Returns a one-row DataFrame with the same schema as :func:`calendar`.

    Examples
    --------
    >>> import pyield as yd
    >>> row = yd.bc.copom.next_meeting()
    >>> len(row)
    1
    """
    ref = clock.today() if reference is None else converter_datas(reference)
    cal = calendar()
    return cal.filter(pl.col("EndDate") >= ref).head(1)
