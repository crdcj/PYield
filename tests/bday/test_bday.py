# pyright: reportGeneralTypeIssues=false

import datetime as dt

import polars as pl

from pyield import bday


def test_count_with_strings1():
    start = "01-01-2023"
    end = "08-01-2023"
    # 01/01/2023 is a Sunday and a holiday
    expected_result = 5
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_strings2():
    start = "15-12-2023"
    end = "02-01-2024"
    # 25/12/2023 is a holiday
    # 01/01/2024 is a holiday
    expected_result = 10
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_with_series():
    start = "01-01-2023"
    end = ["08-01-2023", "22-01-2023"]
    # Assuming no holidays in these periods
    expected_result = [5, 15]
    result = bday.count(start, end)
    assert isinstance(result, pl.Series)
    assert result.to_list() == expected_result, (
        f"Expected {expected_result}, but got {result.to_list()}"
    )


def test_count_negative_count():
    start = "08-01-2023"
    end = "01-01-2023"
    expected_result = -5  # Negative count expected
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_new_holiday():
    start = "20-11-2024"  # Wednesday (Zumbi Nacional Day)
    end = "21-11-2024"
    expected_result = 0
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_old_holiday():
    start = "20-11-2020"  # Friday (was not a holiday in 2020)
    end = "21-11-2020"
    expected_result = 1
    result = bday.count(start, end)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_count_old_and_new_holidays_lists():
    start = ["20-11-2020", "20-11-2024"]
    end = ["21-11-2020", "21-11-2024"]
    expected_result = [1, 0]
    result = bday.count(start, end)
    assert isinstance(result, pl.Series)
    assert result.to_list() == expected_result, (
        f"Expected {expected_result}, but got {result.to_list()}"
    )


def test_offset_with_old_holiday():
    start = "20-11-2020"
    offset = 0
    expected_result = dt.date(2020, 11, 20)
    result = bday.offset(start, offset)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_offset_with_new_holiday():
    start = "20-11-2024"
    offset = 0
    expected_result = dt.date(2024, 11, 21)
    result = bday.offset(start, offset)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_offset_with_old_and_new_holidays():
    start = ["20-11-2020", "20-11-2024"]
    offset = 0
    expected_result = [dt.date(2020, 11, 20), dt.date(2024, 11, 21)]
    result = bday.offset(start, offset)
    assert isinstance(result, pl.Series)
    # Polars Series of dates returns python date objects in to_list()
    assert result.to_list() == expected_result, (
        f"Expected {expected_result}, but got {result.to_list()}"
    )


def test_count_iso_format():
    """Verifica que datas em formato ISO YYYY-MM-DD funcionam corretamente.

    Compara resultados com o formato brasileiro já testado para garantir
    equivalência. Usa um período simples sem feriados de transição.
    """
    start_iso = "2024-01-02"  # terça-feira
    end_iso = "2024-01-09"  # terça-feira seguinte
    # Business days: 02,03,04,05,08 = 5 (06/07 são fim de semana)
    expected = 5
    result_iso = bday.count(start_iso, end_iso)
    assert result_iso == expected

    # Comparação com formato brasileiro dd-mm-YYYY equivalente
    start_br = "02-01-2024"
    end_br = "09-01-2024"
    result_br = bday.count(start_br, end_br)
    assert result_br == expected
    assert result_iso == result_br


def test_count_mixed_string_formats_and_invalid_value():
    start = ["02-01-2024", "03/01/2024", "31-02-2024"]
    end = ["2024-01-09", "2024-01-10", "2024-01-10"]
    result = bday.count(start, end)
    assert isinstance(result, pl.Series)
    assert result.to_list() == [5, 5, None]


def test_offset_mixed_string_formats_and_invalid_value():
    dates = ["02-01-2024", "03/01/2024", "2024-01-04", "31-02-2024"]
    result = bday.offset(dates, 0)
    assert isinstance(result, pl.Series)
    assert result.to_list() == [
        dt.date(2024, 1, 2),
        dt.date(2024, 1, 3),
        dt.date(2024, 1, 4),
        None,
    ]


def test_is_business_day_mixed_string_formats_and_invalid_value():
    dates = ["02-01-2024", "03/01/2024", "2024-01-06", "31-02-2024"]
    result = bday.is_business_day(dates)
    assert isinstance(result, pl.Series)
    assert result.to_list() == [True, True, False, None]


def test_generate_invalid_start_falls_back_to_today(monkeypatch):
    data_fixa = dt.date(2024, 3, 1)
    monkeypatch.setattr("pyield.bday.core.clock.today", lambda: data_fixa)

    result = bday.generate(start="31-02-2024", end="04-03-2024")
    expected = bday.generate(start=None, end="04-03-2024")

    assert result.equals(expected)


def test_generate_invalid_end_falls_back_to_today(monkeypatch):
    data_fixa = dt.date(2024, 3, 4)
    monkeypatch.setattr("pyield.bday.core.clock.today", lambda: data_fixa)

    result = bday.generate(start="01-03-2024", end="31-02-2024")
    expected = bday.generate(start="01-03-2024", end=None)

    assert result.equals(expected)
