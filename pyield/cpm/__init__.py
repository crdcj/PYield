"""CPM: opções digitais da B3 sobre a decisão do COPOM.

Membros:
    contratos: contratos CPM negociados em uma data.
    probabilidades: probabilidades implícitas de cada reunião do COPOM.
"""

from pyield.cpm._contratos import contratos
from pyield.cpm._probabilidades import probabilidades

__all__ = [
    "contratos",
    "probabilidades",
]
