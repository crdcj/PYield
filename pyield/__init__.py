import logging
from importlib.metadata import PackageNotFoundError, version

from pyield import anbima, b3, bc, du, ipca, selic, tn
from pyield.b3 import di1
from pyield.fwd import forward, forwards
from pyield.interpolador import Interpolador
from pyield.relogio import agora, hoje
from pyield.selic.cpm import data as copom_options
from pyield.tn import lft, ltn, ntnb, ntnb1, ntnbprinc, ntnc, ntnf

try:
    __version__ = version("pyield")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "__version__",
    "agora",
    "anbima",
    "b3",
    "bc",
    "copom_options",
    "di1",
    "du",
    "forward",
    "forwards",
    "hoje",
    "Interpolador",
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
