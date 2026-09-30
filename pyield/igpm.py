"""Taxas mensais do IGP-M: consulta por competência ou série histórica."""

from pyield.bc.sgs import igpm_taxa as taxa
from pyield.bc.sgs import igpm_taxa_serie as taxa_serie

__all__ = ["taxa", "taxa_serie"]
