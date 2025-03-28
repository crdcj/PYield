import logging

from pyield import anbima, b3, bc, bday, ibge
from pyield.__about__ import __version__
from pyield.b3 import futures
from pyield.b3.di import DIFutures
from pyield.forward import forwards
from pyield.interpolator import Interpolator
from pyield.tpf import lft, ltn, ntnb, ntnc, ntnf

__all__ = [
    "__version__",
    "anbima",
    "bday",
    "DIFutures",
    "forwards",
    "futures",
    "ibge",
    "Interpolator",
    "lft",
    "ltn",
    "ntnb",
    "ntnc",
    "ntnf",
    "bc",
    "b3",
]

# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
