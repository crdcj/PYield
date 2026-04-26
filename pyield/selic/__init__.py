"""
selic — Dados e análises relacionados à taxa Selic e política monetária.

Submódulos
----------
copom
    Calendário de reuniões do COPOM (BCB).
compromissada
    Leilões de operações compromissadas do BCB.
cpm
    Dados brutos de contratos CPM (opções digitais de COPOM) da B3.
probabilities
    Probabilidades implícitas de mudança de meta nas reuniões do COPOM.
"""

from pyield.bc.sgs import selic_meta as meta
from pyield.bc.sgs import selic_meta_serie as meta_serie
from pyield.bc.sgs import selic_over as over
from pyield.bc.sgs import selic_over_serie as over_serie
from pyield.selic import compromissada, copom, cpm, probabilities
from pyield.selic.compromissada import compromissadas

__all__ = [
    "compromissada",
    "compromissadas",
    "copom",
    "cpm",
    "meta",
    "meta_serie",
    "over",
    "over_serie",
    "probabilities",
]
