from collections.abc import Sequence

import polars as pl

from pyield._internal.types import any_is_empty


def normalizar_contrato(contrato: str | object) -> str:
    """Normaliza um contrato único para caixa alta.

    Args:
        contrato: Contrato único.

    Returns:
        str: Código normalizado em caixa alta ou string vazia se vazio.
    """
    if any_is_empty(contrato):
        return ""

    codigo = str(contrato).strip().upper()
    return codigo


def normalizar_contratos(
    contrato: str | Sequence[str] | pl.Series,
) -> list[str]:
    """Normaliza contratos para lista única em caixa alta.

    Remove valores vazios e duplicados, preservando a ordem de entrada.

    Args:
        contrato: Contrato único ou coleção de contratos.

    Returns:
        list[str]: Lista normalizada de códigos em caixa alta.
    """
    if isinstance(contrato, str):
        codigo = normalizar_contrato(contrato)
        return [codigo] if codigo else []

    if isinstance(contrato, pl.Series):
        valores = contrato.to_list()
    else:
        valores = list(contrato)

    codigos_unicos: list[str] = []
    codigos_vistos: set[str] = set()
    for valor in valores:
        codigo = normalizar_contrato(valor)
        if not codigo:
            continue
        if codigo not in codigos_vistos:
            codigos_vistos.add(codigo)
            codigos_unicos.append(codigo)

    return codigos_unicos
