"""Helpers compartilhados para acesso à API OData do BCB (olinda.bcb.gov.br)."""

import polars as pl
import requests

from pyield._internal.retry import retry_padrao


def montar_url(url_base: str, parametros: dict[str, str]) -> str:
    """Monta URL OData com parâmetros opcionais e formato CSV.

    Args:
        url_base: URL base do endpoint OData (com '?' no final).
        parametros: Dicionário ``{nome_param: valor}`` já formatado.
            Parâmetros com valor vazio são ignorados.
    """
    partes = [f"@{k}='{v}'" for k, v in parametros.items() if v]
    return url_base + "&".join(partes) + "&$format=text/csv"


@retry_padrao
def buscar_csv(url: str) -> bytes:
    """Busca CSV da API OData do BCB com retry automático."""
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.content


def parsear_csv(dados: bytes) -> pl.DataFrame:
    """Lê CSV OData como DataFrame sem inferência de tipos."""
    if not dados.strip():
        return pl.DataFrame()
    return pl.read_csv(dados, infer_schema=False, null_values=["null", ""])
