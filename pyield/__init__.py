import logging

from pyield import anbima, bc, bday
from pyield.__about__ import __version__
from pyield.b3_futures import futures
from pyield.b3_futures.di import DIFutures
from pyield.indicators import indicator
from pyield.interpolator import Interpolator
from pyield.tools import forward_rates
from pyield.tpf import lft, ltn, ntnb, ntnf

__all__ = [
    "__version__",
    "anbima",
    "bday",
    "DIFutures",
    "forward_rates",
    "futures",
    "indicator",
    "Interpolator",
    "lft",
    "ltn",
    "ntnb",
    "ntnf",
    "bc",
]

# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
