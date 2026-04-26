# ruff: noqa: I001

import logging
from importlib.metadata import PackageNotFoundError, version

from pyield import du, ipca
from pyield.b3 import futuro
from pyield.b3 import di1
from pyield.b3.di_over import di_over

# Ordem intencional: alguns módulos importam `di1` a partir de `pyield`
# durante a inicialização do pacote.
from pyield import selic, tn, tpf
from pyield.bc import copom
from pyield.bc.compromissada import compromissadas
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
    "compromissadas",
    "copom",
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
