import logging

from pyield import anbima, b3, bc, bday, ipca, selic, tn
from pyield.__about__ import __version__
from pyield.b3 import di1, futures, futures_intraday
from pyield.clock import now, today
from pyield.fwd import forward, forwards
from pyield.interpolator import Interpolator
from pyield.rmd import rmd
from pyield.selic.cpm import data as copom_options
from pyield.tn import lft, ltn, ntnb, ntnb1, ntnbprinc, ntnc, ntnf, pre

__all__ = [
    "__version__",
    "anbima",
    "bc",
    "b3",
    "ipca",
    "tn",
    "bday",
    "di1",
    "forwards",
    "forward",
    "futures",
    "copom_options",
    "Interpolator",
    "today",
    "now",
    "lft",
    "ltn",
    "ntnb",
    "ntnbprinc",
    "ntnb1",
    "ntnc",
    "ntnf",
    "pre",
    "rmd",
    "bday",
    "selic",
    "futures_intraday",
]

# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
