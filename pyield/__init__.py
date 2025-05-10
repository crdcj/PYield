import logging

from pyield import anbima, b3, bc, bday, ibge
from pyield.__about__ import __version__
from pyield.b3 import di1, futures
from pyield.fwd import forward, forwards
from pyield.interpolator import Interpolator
from pyield.tpf import lft, ltn, ntnb, ntnc, ntnf

__all__ = [
    "__version__",
    "anbima",
    "bday",
    "di1",
    "forwards",
    "forward",
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
