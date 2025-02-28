import logging
import urllib.error

import tenacity

from pyield import anbima, bc, bday
from pyield.__about__ import __version__
from pyield.b3_futures import futures
from pyield.b3_futures.di import DIFutures
from pyield.indicators import indicator
from pyield.interpolator import Interpolator
from pyield.projections import projection
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
    "projection",
    "bc",
]

# Configura o logger do pacote principal com um NullHandler
logging.getLogger(__name__).addHandler(logging.NullHandler())

global_logger = logging.getLogger("pyield")

global_retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type(urllib.error.URLError),
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=tenacity.before_sleep_log(global_logger, logging.WARNING),
)
