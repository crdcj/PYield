"""Módulo interno. Não faz parte da API pública."""

import datetime as dt
from pathlib import Path
from typing import Literal

import polars as pl


class BrHolidays:
    """Calendário de feriados nacionais (lista antiga e nova).

    Uso interno do módulo `bday`.
    DATA_TRANSICAO (inclusive): 2023-12-26. Antes desta data usa lista antiga.
    A partir desta data usa lista nova.
    """

    DATA_TRANSICAO = dt.date(2023, 12, 26)

    def __init__(self) -> None:
        base = Path(__file__).parent
        self.feriados_novos = self._carregar_feriados(base / "br_holidays_new.txt")
        self.feriados_antigos = self._carregar_feriados(base / "br_holidays_old.txt")

    @staticmethod
    def _carregar_feriados(caminho_arquivo: Path) -> list[dt.date]:
        df = pl.read_csv(
            caminho_arquivo,
            has_header=False,
            new_columns=["date"],
            comment_prefix="#",
        ).with_columns(pl.col("date").str.to_date(format="%d/%m/%Y"))
        return df["date"].to_list()

    def obter_feriados(
        self,
        datas: dt.date | pl.Series | None = None,
        opcao_feriado: Literal["old", "new", "infer"] = "infer",
    ) -> list[dt.date]:
        """Retorna a lista de feriados conforme opção ou inferência.

        datas: data única ou série de datas para inferir (quando
            opcao_feriado='infer').
        opcao_feriado: 'old', 'new' ou 'infer'.
        """
        match opcao_feriado:
            case "old":
                return self.feriados_antigos
            case "new":
                return self.feriados_novos
            case "infer":
                if datas is None:
                    raise ValueError("'datas' obrigatório em 'infer'.")
                if isinstance(datas, dt.date):
                    data_minima = datas
                else:
                    data_minima = datas.drop_nulls().min()

                if not isinstance(data_minima, dt.date):
                    raise ValueError("Não foi possível inferir a data mínima.")

                if data_minima < self.DATA_TRANSICAO:
                    return self.feriados_antigos
                else:
                    return self.feriados_novos

            case _:
                raise ValueError("Opção inválida para holiday_option.")
