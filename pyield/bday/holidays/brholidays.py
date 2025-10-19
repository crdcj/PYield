import datetime as dt
from pathlib import Path
from typing import Literal

import polars as pl


class BrHolidays:
    """Calendário de feriados nacionais (lista antiga e nova).

    TRANSITION_DATE (inclusive): 2023-12-26. Antes desta data usa lista antiga.
    A partir desta data usa lista nova.
    """

    TRANSITION_DATE = dt.date(2023, 12, 26)

    def __init__(self) -> None:
        base = Path(__file__).parent
        self.new_holidays = self._load_holidays(base / "br_holidays_new.txt")
        self.old_holidays = self._load_holidays(base / "br_holidays_old.txt")

    @staticmethod
    def _load_holidays(file_path: Path) -> pl.Series:
        df = pl.read_csv(
            file_path,
            has_header=False,
            new_columns=["date"],
            comment_prefix="#",
        ).with_columns(
            pl.col("date").str.strptime(pl.Date, format="%d/%m/%Y", strict=True)
        )
        return df["date"]

    def get_holiday_series(
        self,
        dates: dt.date | pl.Series | None = None,
        holiday_option: Literal["old", "new", "infer"] = "infer",
    ) -> pl.Series:
        """Retorna a série de feriados conforme opção ou inferência.

        dates: data única ou série de datas para inferir (quando
            holiday_option='infer').
        holiday_option: 'old', 'new' ou 'infer'.
        """
        match holiday_option:
            case "old":
                return self.old_holidays
            case "new":
                return self.new_holidays
            case "infer":
                if dates is None:
                    raise ValueError("'dates' obrigatório em 'infer'.")
                if isinstance(dates, dt.date):
                    earliest = dates
                else:
                    earliest = dates.drop_nulls().min()
                return (
                    self.old_holidays
                    if earliest < self.TRANSITION_DATE
                    else self.new_holidays
                )
            case _:
                raise ValueError("Opção inválida para holiday_option.")
