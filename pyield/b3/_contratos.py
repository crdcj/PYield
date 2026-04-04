from collections.abc import Sequence

import polars as pl

from pyield._internal.types import any_is_empty


def normalizar_codigo_contrato(contract_code: str | object) -> str:
    """Normaliza um código de contrato único para caixa alta.

    Args:
        contract_code: Código único de contrato.

    Returns:
        str: Código normalizado em caixa alta ou string vazia se vazio.
    """
    if any_is_empty(contract_code):
        return ""

    codigo = str(contract_code).strip().upper()
    return codigo


def normalizar_codigos_contrato(
    contract_code: str | Sequence[str] | pl.Series,
) -> list[str]:
    """Normaliza códigos de contrato para lista única em caixa alta.

    Remove valores vazios e duplicados, preservando a ordem de entrada.

    Args:
        contract_code: Código único ou coleção de códigos de contrato.

    Returns:
        list[str]: Lista normalizada de códigos em caixa alta.
    """
    if isinstance(contract_code, str):
        codigo = normalizar_codigo_contrato(contract_code)
        return [codigo] if codigo else []

    if isinstance(contract_code, pl.Series):
        valores = contract_code.to_list()
    else:
        valores = list(contract_code)

    codigos_unicos: list[str] = []
    codigos_vistos: set[str] = set()
    for valor in valores:
        codigo = normalizar_codigo_contrato(valor)
        if not codigo:
            continue
        if codigo not in codigos_vistos:
            codigos_vistos.add(codigo)
            codigos_unicos.append(codigo)

    return codigos_unicos
