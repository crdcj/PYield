import logging

from pyield import anbima, b3, bc, bday, ibge, tn
from pyield.__about__ import __version__
from pyield.b3 import di1, futures
from pyield.fwd import forward, forwards
from pyield.interpolator import Interpolator
from pyield.tn import lft, ltn, ntnb, ntnc, ntnf

__all__ = [
    "__version__",
    "anbima",
    "bc",
    "b3",
    "ibge",
    "tn",
    "bday",
    "di1",
    "forwards",
    "forward",
    "futures",
    "Interpolator",
    "lft",
    "ltn",
    "ntnb",
    "ntnc",
    "ntnf",
]

# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
