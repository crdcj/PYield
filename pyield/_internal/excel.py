"""Leitura interna de planilhas Excel."""

import fastexcel
import polars as pl


def ler_sem_cabecalho(conteudo: bytes, aba: int | str = 0) -> pl.DataFrame:
    """Lê uma aba sem cabeçalho e atribui nomes posicionais às colunas."""
    planilha = fastexcel.read_excel(conteudo)
    df = pl.DataFrame(planilha.load_sheet(aba, header_row=None)).filter(
        pl.any_horizontal(pl.all().is_not_null())
    )
    return df.rename(
        {nome: f"column_{indice}" for indice, nome in enumerate(df.columns, start=1)}
    )
