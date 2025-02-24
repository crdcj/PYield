import logging

from pyield import bday
from pyield.__about__ import __version__
from pyield.anbima.ima import last_ima
from pyield.anbima.imaq import imaq
from pyield.anbima.tpf import anbima_tpf_data, anbima_tpf_rates, tpf_pre_maturities
from pyield.b3_futures import futures
from pyield.b3_futures.di import DIFutures
from pyield.bc.auction import auctions
from pyield.bc.ptax import ptax
from pyield.bc.repos import repos
from pyield.indicators import indicator
from pyield.interpolator import Interpolator
from pyield.projections import projection
from pyield.tools import forward_rates
from pyield.tpf import lft, ltn, ntnb, ntnf

__all__ = [
    "__version__",
    "anbima_tpf_data",
    "anbima_tpf_rates",
    "tpf_pre_maturities",
    "auctions",
    "repos",
    "ptax",
    "bday",
    "DIFutures",
    "forward_rates",
    "futures",
    "last_ima",
    "imaq",
    "indicator",
    "Interpolator",
    "lft",
    "ltn",
    "ntnb",
    "ntnf",
    "projection",
]


# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
