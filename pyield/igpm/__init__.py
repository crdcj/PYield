"""Taxas, números-índice e calendário de divulgação do IGP-M.

Disponibiliza ``taxa``, ``taxa_serie``, ``indice``, ``indice_serie`` e
``calendario``. As taxas vêm do SGS/BCB; os índices e o calendário têm
implementações próprias neste pacote.
"""

from pyield.bc.sgs import igpm_taxa as taxa
from pyield.bc.sgs import igpm_taxa_serie as taxa_serie
from pyield.igpm._calendario import calendario
from pyield.igpm._indices import indice, indice_serie

__all__ = ["taxa", "taxa_serie", "indice", "indice_serie", "calendario"]
