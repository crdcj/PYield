import logging
import urllib.error

import tenacity

global_logger = logging.getLogger("pyield")


global_retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type(urllib.error.URLError),
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=tenacity.before_sleep_log(global_logger, logging.WARNING),
)
