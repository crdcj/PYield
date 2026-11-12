"""Taxas, números-índice, projeção e calendário de divulgação do IPCA.

Disponibiliza ``taxa``, ``taxa_serie``, ``indice``, ``indice_serie``,
``taxa_projetada`` e ``calendario``.
"""

from pyield.anbima.projecoes import ipca_taxa_projetada as taxa_projetada
from pyield.ipca._calendario import calendario
from pyield.ipca.historico import (
    indice,
    indice_serie,
    taxa,
    taxa_serie,
)

__all__ = [
    "calendario",
    "indice",
    "indice_serie",
    "taxa",
    "taxa_projetada",
    "taxa_serie",
]
