import datetime as dt
from typing import overload

import polars as pl

from pyield import du
from pyield._internal.types import ArrayLike, any_is_collection

# Lista de contratos que negociam por taxa (juros/cupom).
# Nestes contratos, as colunas OHLC são taxas e precisam ser divididas por 100.
CONTRATOS_TAXA = {"DI1", "DAP", "DDI", "FRC", "FRO"}

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


def dv01_expr(
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


@overload
def vencimento(codigo: str, contrato: str) -> dt.date | None: ...
@overload
def vencimento(codigo: None, contrato: str) -> None: ...
@overload
def vencimento(codigo: ArrayLike, contrato: str) -> pl.Series: ...


def vencimento(
    codigo: str | ArrayLike | None,
    contrato: str,
) -> dt.date | pl.Series | None:
    """Calcula a data de vencimento de contratos futuros da B3.

    Args:
        codigo: Código de negociação ou coleção de códigos.
        contrato: Contrato futuro na B3 (ex.: ``DI1`` ou ``DAP``).

    Returns:
        Data de vencimento para código escalar, ou ``pl.Series`` para coleção.

    Examples:
        >>> yd.futuro.vencimento("DI1F25", "DI1")
        datetime.date(2025, 1, 2)

        >>> yd.futuro.vencimento(["DI1F25", "di1g25", "DI1E27"], "DI1")
        shape: (3,)
        Series: 'vencimento' [date]
        [
            2025-01-02
            2025-02-03
            null
        ]
    """
    dados = codigo if any_is_collection(codigo) else [codigo]
    serie = (
        pl.DataFrame({"codigo": dados})
        .select(vencimento=vencimento_expr("codigo", contrato))
        .get_column("vencimento")
    )

    if any_is_collection(codigo):
        return serie

    return serie.item()


def vencimento_expr(coluna_codigo: str, contrato: str) -> pl.Expr:
    """Cria expressão Polars para a data de vencimento de futuros da B3.

    Assume códigos de negociação no formato padrão de futuros da B3 (ex.:
    ``DI1F25``). A caixa dos caracteres é ignorada e o vencimento é ajustado
    para dia útil.

    Args:
        coluna_codigo: Nome da coluna com o código de negociação.
        contrato: Contrato futuro na B3 (ex.: ``DI1`` ou ``DAP``).

    Returns:
        Uma ``pl.Expr`` que resulta em Date.

    Examples:
        >>> df = pl.DataFrame({"codigo_negociacao": ["DI1F25", "di1g25", "DI1E27"]})
        >>> df.select(
        ...     yd.futuro.vencimento_expr("codigo_negociacao", "DI1").alias(
        ...         "vencimento"
        ...     )
        ... )
        shape: (3, 1)
        ┌────────────┐
        │ vencimento │
        │ ---        │
        │ date       │
        ╞════════════╡
        │ 2025-01-02 │
        │ 2025-02-03 │
        │ null       │
        └────────────┘

        Contratos DAP vencem no dia 15:
        >>> df = pl.DataFrame({"codigo_negociacao": ["DAPF25"]})
        >>> df.select(
        ...     yd.futuro.vencimento_expr("codigo_negociacao", "DAP").alias(
        ...         "vencimento"
        ...     )
        ... )
        shape: (1, 1)
        ┌────────────┐
        │ vencimento │
        │ ---        │
        │ date       │
        ╞════════════╡
        │ 2025-01-15 │
        └────────────┘
    """
    dia_vencimento = 15 if "DAP" in contrato.upper() else 1
    codigo = pl.col(coluna_codigo).str.to_uppercase()
    data_vencimento = pl.date(
        year=codigo.str.slice(4, 2).cast(pl.Int32, strict=False) + 2000,
        month=codigo.str.slice(3, 1).replace_strict(
            _MAPA_MESES, default=None, return_dtype=pl.Int8
        ),
        day=dia_vencimento,
    )
    return du.deslocar_expr(data_vencimento, 0)
