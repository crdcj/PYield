import datetime as dt

import polars as pl
import pytest

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


def test_count_mixed_formats_error():
    """Mistura de formatos em uma coleção deve levantar ValueError.

    A lógica em `convert_dates` detecta formato com base no primeiro não-nulo
    e aplica parsing fixo. Ao incluir um segundo formato diferente deve falhar.
    """
    mixed_dates = ["02-01-2024", "2024-01-03", "04-01-2024"]
    end = "09-01-2024"
    # A mensagem vem do pandas (array_strptime) ao tentar aplicar fmt uniforme.
    # Procuramos trecho chave indicando mismatch de formato.
    with pytest.raises(ValueError, match="doesn't match format") as exc_info:
        bday.count(mixed_dates, end)
    # Mensagem deve indicar formato inválido para a string ISO sendo
    # interpretada com formato brasileiro.
    assert "doesn't match format" in str(exc_info.value)
