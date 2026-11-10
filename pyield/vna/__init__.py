"""Consulta e cálculo de VNA por título.

Disponibiliza ``valor``, ``historico``, ``ultimo``, ``vigencia``, ``projetado``,
``contexto``, ``calcular_vna`` e ``calcular_base``.
"""

from pyield.vna._calculo import calcular_vna
from pyield.vna._consulta import (
    TipoTitulo,
    calcular_base,
    historico,
    projetado,
    ultimo,
    valor,
    vigencia,
)
from pyield.vna._contexto import contexto

__all__ = [
    "contexto",
    "calcular_base",
    "calcular_vna",
    "TipoTitulo",
    "historico",
    "projetado",
    "ultimo",
    "valor",
    "vigencia",
]
