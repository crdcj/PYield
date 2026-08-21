"""Módulo interno. Não faz parte da API pública."""

import datetime as dt
from pathlib import Path
from typing import Literal

import polars as pl


class FeriadosBrasil:
    """Calendário de feriados nacionais (lista anterior e atual).

    Uso interno do módulo `dus`.
    DATA_TRANSICAO (inclusive): 2023-12-26. Antes desta data usa lista anterior.
    A partir desta data usa a lista atual.
    """

    DATA_TRANSICAO = dt.date(2023, 12, 26)

    def __init__(self) -> None:
        base = Path(__file__).parent
        self.feriados_atuais = self._carregar_feriados(
            base / "feriados_novos_br.txt"
        )
        self.feriados_anteriores = self._carregar_feriados(
            base / "feriados_antigos_br.txt"
        )

    @staticmethod
    def _carregar_feriados(caminho_arquivo: Path) -> list[dt.date]:
        df = pl.read_csv(
            caminho_arquivo,
            has_header=False,
            new_columns=["data"],
            comment_prefix="#",
        ).with_columns(pl.col("data").str.to_date(format="%d/%m/%Y"))
        return df["data"].to_list()

    def obter_feriados(
        self,
        datas: dt.date | pl.Series | None = None,
        calendario: Literal["auto", "anterior", "atual"] = "auto",
    ) -> list[dt.date]:
        """Retorna a lista de feriados conforme a opção selecionada.

        datas: Data única ou série de datas para seleção automática.
        calendario: ``"auto"``, ``"anterior"`` ou ``"atual"``.
        """
        match calendario:
            case "anterior":
                return self.feriados_anteriores
            case "atual":
                return self.feriados_atuais
            case "auto":
                if datas is None:
                    raise ValueError(
                        "'datas' é obrigatório quando calendario='auto'."
                    )
                if isinstance(datas, dt.date):
                    data_minima = datas
                else:
                    data_minima = datas.drop_nulls().min()

                if not isinstance(data_minima, dt.date):
                    raise ValueError("Não foi possível selecionar o calendário.")

                if data_minima < self.DATA_TRANSICAO:
                    return self.feriados_anteriores
                else:
                    return self.feriados_atuais

            case _:
                raise ValueError("Opção inválida para calendario.")
