import logging
from importlib.metadata import PackageNotFoundError, version

from pyield import b3, copom, cpm, du, futuro, igpm, ipca, selic, tpf, vna
from pyield.bc.sgs import di_over, di_over_serie, ptax, ptax_serie
from pyield.compromissada import compromissadas
from pyield.futuro import di1
from pyield.fwd import forward, forwards, forwards_expr
from pyield.interpolador import Interpolador, interpolar
from pyield.relogio import agora, hoje
from pyield.rmd import rmd
from pyield.tpf.titulos import lft, ltn, ntnb, ntnb1, ntnbp, ntnc, ntnf

try:
    __version__ = version("pyield")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "__version__",
    "agora",
    "b3",
    "compromissadas",
    "cpm",
    "copom",
    "di1",
    "di_over",
    "di_over_serie",
    "du",
    "forward",
    "forwards",
    "forwards_expr",
    "futuro",
    "hoje",
    "igpm",
    "Interpolador",
    "interpolar",
    "ipca",
    "lft",
    "ltn",
    "ntnb",
    "ntnb1",
    "ntnbp",
    "ntnc",
    "ntnf",
    "ptax",
    "ptax_serie",
    "rmd",
    "selic",
    "tpf",
    "vna",
]


# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())
