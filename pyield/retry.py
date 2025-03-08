import logging
from urllib.error import HTTPError, URLError

import pandas as pd
import tenacity

logger = logging.getLogger(__name__)

# Default timeout values for HTTP requests (connect_timeout, read_timeout)
DEFAULT_TIMEOUT = (3.05, 30)


def retry_if_not_404(exception):
    """
    Retry if the exception is not a 404 error.

    Args:
        exception: The exception that was raised

    Returns:
        Boolean indicating whether to retry
    """
    # Don't retry for 404 errors (resource doesn't exist)
    if isinstance(exception, HTTPError) and exception.code == 404:  # noqa
        logger.info("Resource not found (404), not retrying.")
        return False
    # Retry for parsing errors and other connection errors
    return isinstance(
        exception, (URLError, pd.errors.EmptyDataError, pd.errors.ParserError)
    )


# Custom retry configuration that handles parsing errors
default_retry = tenacity.retry(
    retry=tenacity.retry_if_exception(retry_if_not_404),
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=tenacity.before_sleep_log(logger, logging.WARNING),
)
