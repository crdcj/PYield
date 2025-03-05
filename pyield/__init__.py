import logging

from pyield import anbima, b3, bc, bday, ibge
from pyield.__about__ import __version__
from pyield.b3 import futures
from pyield.b3.di import DIFutures
from pyield.forwards import forward_rates
from pyield.interpolator import Interpolator
from pyield.tpf import lft, ltn, ntnb, ntnf

__all__ = [
    "__version__",
    "anbima",
    "bday",
    "DIFutures",
    "forward_rates",
    "futures",
    "ibge",
    "Interpolator",
    "lft",
    "ltn",
    "ntnb",
    "ntnf",
    "bc",
    "b3",
]

# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
