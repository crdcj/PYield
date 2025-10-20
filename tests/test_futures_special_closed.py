import polars as pl

import pyield as yd


def test_christmas_eve_returns_empty_df():
    # 24-12-2024 é véspera de Natal (terça) mas sem pregão
    df = yd.futures(contract_code="DI1", date="24-12-2024")
    assert isinstance(df, pl.DataFrame)
    assert df.is_empty(), (
        "Expected empty DataFrame for Christmas Eve (no trading session)"
    )


def test_new_years_eve_returns_empty_df():
    df = yd.futures(contract_code="DI1", date="31-12-2024")
    assert df.is_empty(), (
        "Expected empty DataFrame for New Year's Eve (no trading session)"
    )
