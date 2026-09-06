"""
selic — Dados e análises relacionados à taxa Selic e política monetária.

Submódulos
----------
cpm
    Dados brutos de contratos CPM (opções digitais de COPOM) da B3.
probabilities
    Probabilidades implícitas de mudança de meta nas reuniões do COPOM.
"""

from pyield.bc.sgs import selic_meta as meta
from pyield.bc.sgs import selic_meta_serie as meta_serie
from pyield.bc.sgs import selic_over as over
from pyield.bc.sgs import selic_over_serie as over_serie
from pyield.selic import cpm, probabilities

__all__ = [
    "cpm",
    "meta",
    "meta_serie",
    "over",
    "over_serie",
    "probabilities",
]
