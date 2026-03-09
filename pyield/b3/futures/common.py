import polars as pl

from pyield import bday

_MAPA_MESES: dict[str, int] = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}
def expr_dv01(
    coluna_dias_uteis: str,
    coluna_taxa: str,
    coluna_preco: str,
) -> pl.Expr:
    """Retorna a expressão Polars para cálculo de DV01.

    Fórmula:
    DV01 = (Duration / (1 + taxa)) * preço * 0,0001

    Onde:
    - Duration = dias_uteis / 252
    - taxa deve estar em formato decimal (ex.: 0.145)
    - preço é o PU do contrato.
    """
    duracao = pl.col(coluna_dias_uteis) / 252
    duracao_modificada = duracao / (1 + pl.col(coluna_taxa))
    return 0.0001 * duracao_modificada * pl.col(coluna_preco)


def adicionar_vencimento(
    df: pl.DataFrame, codigo_contrato: str, coluna_ticker: str
) -> pl.DataFrame:
    """
    Recebe um DataFrame Polars e ADICIONA a coluna 'ExpirationDate'.

    - Pega a coluna 'coluna_ticker'.
    - Extrai o código de vencimento.
    - Converte para a data "bruta", sem ajuste de feriado.
    - Garante que a data de vencimento é um dia útil.
    - Retorna o DataFrame com a nova coluna ExpirationDate.

    Assume tickers no formato padrão de futuros da B3 (ex.: DI1F25).
    """
    dia_vencimento = 15 if "DAP" in codigo_contrato else 1
    df = df.with_columns(
        pl.date(
            # Ano: posição 4-5 (2 dígitos) -> Int -> Soma 2000
            # Funciona para tickers padrão de futuros (ex.: DI1F25)
            year=pl.col(coluna_ticker).str.slice(4, 2).cast(pl.Int32, strict=False)
            + 2000,
            # Mês: posição 3 (1 char = código do mês) -> mapeia para Int
            month=pl.col(coluna_ticker)
            .str.slice(3, 1)
            .replace_strict(_MAPA_MESES, default=None, return_dtype=pl.Int8),
            day=dia_vencimento,
        ).alias("ExpirationDate")
    )
    # Garante que a data de vencimento é um dia útil
    df = df.with_columns(ExpirationDate=bday.offset_expr("ExpirationDate", 0))

    return df
