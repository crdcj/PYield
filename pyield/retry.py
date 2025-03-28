import logging
import time
from urllib.error import HTTPError, URLError

import pandas as pd
import tenacity

logger = logging.getLogger(__name__)

# Default timeout values for HTTP requests (connect_timeout, read_timeout)
DEFAULT_TIMEOUT = (2, 10)


def retry_if_recoverable(exception):
    """
    Determine if the exception is worth retrying.
    """
    if isinstance(exception, HTTPError):
        # Don't retry for 404 errors
        if exception.code == 404:  # noqa
            logger.info("Resource not found (404), not retrying.")
            return False

        # Don't retry for 504 Gateway Timeout after first attempt
        # These often indicate longer-term issues with API
        if exception.code == 504:  # noqa
            logger.warning(
                "Gateway Timeout (504) from API - likely service degradation."
            )
            # Check if we've already retried for this 504
            retry_state = getattr(retry_if_recoverable, "retry_for_504", 0)
            if retry_state >= 1:
                logger.error("Multiple 504 errors - BC API appears to be unavailable.")
                return False
            # Allow just one retry for 504 with a longer wait
            retry_if_recoverable.retry_for_504 = retry_state + 1
            time.sleep(2)  # Immediate wait before retry
            return True

    # Reset the 504 counter for other types of errors
    retry_if_recoverable.retry_for_504 = 0

    # Retry for other HTTP errors, URLErrors, and parsing errors
    return isinstance(
        exception,
        (HTTPError, URLError, pd.errors.EmptyDataError, pd.errors.ParserError),
    )


# Custom retry configuration that handles parsing errors
default_retry = tenacity.retry(
    retry=tenacity.retry_if_exception(retry_if_recoverable),
    stop=(tenacity.stop_after_attempt(3) | tenacity.stop_after_delay(15)),
    wait=tenacity.wait_exponential(multiplier=0.5, min=0.5, max=2),
    before_sleep=tenacity.before_sleep_log(logger, logging.WARNING),
)
