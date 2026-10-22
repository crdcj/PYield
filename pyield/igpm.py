"""Taxas e números-índice do IGP-M por competência ou série histórica."""

from pyield._internal.calendario_igpm import calendario
from pyield._internal.igpm import indice, indice_serie
from pyield.bc.sgs import igpm_taxa as taxa
from pyield.bc.sgs import igpm_taxa_serie as taxa_serie

__all__ = ["taxa", "taxa_serie", "indice", "indice_serie", "calendario"]
