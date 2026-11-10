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

    return str(contrato).strip().upper()


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
    valores = [contrato] if isinstance(contrato, str) else contrato
    codigos = (normalizar_contrato(valor) for valor in valores)
    return list(dict.fromkeys(codigo for codigo in codigos if codigo))
