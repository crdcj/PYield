"""
selic — Dados e análises relacionados à taxa Selic e política monetária.

Submódulos
----------
Este namespace reúne os indicadores diretamente relacionados à taxa Selic.
O produto CPM e suas análises ficam disponíveis em ``pyield.cpm``.
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
