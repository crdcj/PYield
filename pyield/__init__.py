import logging
from importlib.metadata import PackageNotFoundError, version

from pyield import anbima, b3, bc, di1, du, futuro, ipca, selic, tn, tpf
from pyield.b3.di_over import di_over
from pyield.bc.sgs import (
    ptax,
    ptax_serie,
    selic_meta,
    selic_meta_serie,
    selic_over,
    selic_over_serie,
)
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
    "di_over",
    "du",
    "forward",
    "forwards",
    "futuro",
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
    "ptax",
    "ptax_serie",
    "selic",
    "selic_meta",
    "selic_meta_serie",
    "selic_over",
    "selic_over_serie",
    "tn",
    "tpf",
]

# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
