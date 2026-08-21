"""Feriados brasileiros usados internamente pelos cálculos de dias úteis."""

import datetime as dt
from pathlib import Path
from typing import Final

import polars as pl

DATA_TRANSICAO: Final = dt.date(2023, 12, 26)


def _carregar(nome_arquivo: str) -> list[dt.date]:
    return (
        pl.read_csv(
            Path(__file__).parent / nome_arquivo,
            has_header=False,
            new_columns=["data"],
            comment_prefix="#",
        )
        .with_columns(pl.col("data").str.to_date(format="%d/%m/%Y"))
        .get_column("data")
        .to_list()
    )


ANTERIORES: Final = _carregar("anteriores.txt")
ATUAIS: Final = _carregar("atuais.txt")
