import logging

from pyield import anbima, b3, bc, dus, ipca, selic, tn
from pyield.__about__ import __version__
from pyield.b3 import di1
from pyield.fwd import forward, forwards
from pyield.interpolador import Interpolador
from pyield.relogio import agora, hoje
from pyield.selic.cpm import data as copom_options
from pyield.tn import lft, ltn, ntnb, ntnb1, ntnbprinc, ntnc, ntnf

__all__ = [
    "Interpolador",
    "__version__",
    "agora",
    "anbima",
    "b3",
    "bc",
    "copom_options",
    "di1",
    "dus",
    "forward",
    "forwards",
    "hoje",
    "ipca",
    "lft",
    "ltn",
    "ntnb",
    "ntnb1",
    "ntnbprinc",
    "ntnc",
    "ntnf",
    "selic",
    "tn",
]

# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
