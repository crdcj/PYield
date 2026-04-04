import logging

from pyield import anbima, b3, bc, dus, ipca, relogio, selic, tn
from pyield.__about__ import __version__
from pyield.fwd import forward, forwards
from pyield.interpolador import Interpolador
from pyield.relogio import agora, hoje
from pyield.selic.cpm import data as copom_options

__all__ = [
    "Interpolador",
    "__version__",
    "anbima",
    "b3",
    "bc",
    "copom_options",
    "dus",
    "forward",
    "forwards",
    "agora",
    "ipca",
    "relogio",
    "selic",
    "tn",
    "hoje",
]

# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
