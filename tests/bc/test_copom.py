"""
Tests for pyield.bc.copom — COPOM meeting calendar.

All tests use the fixture parquet. No live BCB API calls.
"""

import datetime
from pathlib import Path

import polars as pl
import pytest

import pyield.bc.copom as copom
from pyield import bday

DATA = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def calendar_fixture() -> pl.DataFrame:
    return pl.read_parquet(DATA / "copom_calendar.parquet")


@pytest.fixture(scope="module")
def cal(calendar_fixture) -> pl.DataFrame:
    return calendar_fixture


# ── Schema ────────────────────────────────────────────────────────────────


def test_calendar_columns(cal):
    assert cal.columns == ["MeetingNumber", "StartDate", "EndDate", "ExpiryDate"]


def test_calendar_date_dtypes(cal):
    assert cal["StartDate"].dtype == pl.Date
    assert cal["EndDate"].dtype == pl.Date
    assert cal["ExpiryDate"].dtype == pl.Date


def test_meeting_number_dtype(cal):
    assert cal["MeetingNumber"].dtype == pl.Int32


# ── Ordering and uniqueness ───────────────────────────────────────────────


def test_end_date_sorted(cal):
    assert cal["EndDate"].is_sorted()


def test_no_duplicate_end_dates(cal):
    assert cal["EndDate"].n_unique() == len(cal)


# ── ExpiryDate correctness ────────────────────────────────────────────────


def test_expiry_is_one_bday_after_end(cal):
    """ExpiryDate must equal bday.offset(EndDate, 1) for every row."""
    for row in cal.iter_rows(named=True):
        expected = bday.offset(row["EndDate"], 1)
        assert row["ExpiryDate"] == expected, (
            f"MeetingNumber={row['MeetingNumber']}: "
            f"ExpiryDate={row['ExpiryDate']}, expected={expected}"
        )


# ── Future meetings ───────────────────────────────────────────────────────


def test_future_meetings_present(cal):
    """At least one row must have MeetingNumber == null (future)."""
    assert cal["MeetingNumber"].null_count() >= 1


def test_future_meetings_after_past(cal):
    """All future meeting EndDates are after the last past meeting."""
    last_past = cal.filter(pl.col("MeetingNumber").is_not_null())["EndDate"].max()
    future_dates = cal.filter(pl.col("MeetingNumber").is_null())["EndDate"]
    assert (future_dates > last_past).all()


# ── Date range filter ─────────────────────────────────────────────────────


def test_calendar_date_range_filter(monkeypatch, calendar_fixture):
    monkeypatch.setattr(
        copom,
        "_fetch_past_meetings",
        lambda: calendar_fixture.filter(pl.col("MeetingNumber").is_not_null()),
    )
    monkeypatch.setattr(
        copom,
        "_build_future_meetings",
        lambda: calendar_fixture.filter(pl.col("MeetingNumber").is_null()),
    )
    result = copom.calendar(start="2025-01-01", end="2025-12-31")
    assert result["EndDate"].min() >= datetime.date(2025, 1, 1)
    assert result["EndDate"].max() <= datetime.date(2025, 12, 31)


def test_calendar_far_future_returns_empty(monkeypatch, calendar_fixture):
    monkeypatch.setattr(
        copom,
        "_fetch_past_meetings",
        lambda: calendar_fixture.filter(pl.col("MeetingNumber").is_not_null()),
    )
    monkeypatch.setattr(
        copom,
        "_build_future_meetings",
        lambda: calendar_fixture.filter(pl.col("MeetingNumber").is_null()),
    )
    result = copom.calendar(start="2099-01-01")
    assert result.is_empty()


# ── next_meeting ──────────────────────────────────────────────────────────


def test_next_meeting_returns_one_row(monkeypatch, calendar_fixture):
    monkeypatch.setattr(copom, "calendar", lambda **kw: calendar_fixture)
    result = copom.next_meeting(reference="2025-01-01")
    assert len(result) == 1


def test_next_meeting_end_date_after_reference(monkeypatch, calendar_fixture):
    monkeypatch.setattr(copom, "calendar", lambda **kw: calendar_fixture)
    ref = datetime.date(2025, 1, 29)
    result = copom.next_meeting(reference=ref)
    assert result["EndDate"].item() >= ref


def test_next_meeting_far_future_empty(monkeypatch, calendar_fixture):
    monkeypatch.setattr(copom, "calendar", lambda **kw: calendar_fixture)
    result = copom.next_meeting(reference="2099-01-01")
    assert result.is_empty()
