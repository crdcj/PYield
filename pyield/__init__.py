# ruff: noqa: I001

import logging
from importlib.metadata import PackageNotFoundError, version

from pyield import du, ipca
from pyield.b3.di_over import di_over
from pyield import futuro, di1

# Ordem intencional: alguns módulos importam `di1` a partir de `pyield`
# durante a inicialização do pacote.
from pyield import selic, tpf
from pyield.bc.sgs import ptax, ptax_serie
from pyield.fwd import forward, forwards
from pyield.interpolador import Interpolador
from pyield.relogio import agora, hoje
from pyield import lft, ltn, ntnb, ntnb1, ntnbprinc, ntnc, ntnf

try:
    __version__ = version("pyield")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "__version__",
    "agora",
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
    "tpf",
]


# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
