"""Indicadores da taxa Selic: ``over``, ``over_serie``, ``meta`` e ``meta_serie``.

O produto CPM e suas análises ficam em ``pyield.cpm``; o calendário de
reuniões do Copom, em ``pyield.copom``.
"""

from pyield.bc.sgs import selic_meta as meta
from pyield.bc.sgs import selic_meta_serie as meta_serie
from pyield.bc.sgs import selic_over as over
from pyield.bc.sgs import selic_over_serie as over_serie

__all__ = [
    "meta",
    "meta_serie",
    "over",
    "over_serie",
]
