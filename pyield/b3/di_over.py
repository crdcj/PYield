import ftplib
import logging

from pyield.converters import convert_dates
from pyield.types import DateLike, has_nullable_args

logger = logging.getLogger(__name__)

# 4 decimal places in rate = 2 decimal places in percentage
DI_OVER_DECIMAL_PLACES = 4


def di_over(date: DateLike) -> float:
    """
    Gets the DI (Interbank Deposit) rate for a specific date from B3/CETIP FTP server.

    Args:
        date (DateLike): The reference date for fetching the DI rate.

    Returns:
        float: DI rate for the specified date (e.g., 0.1315 for 13.15%).
               Returns float("nan") if the file is not found (e.g., weekends, holidays).

    Raises:
        ValueError: If date is not in the correct format.
        ConnectionError: If connection to the FTP server fails or other transfer
            errors occur.

    Examples:
        >>> di_over("28/02/2025")
        0.1315
        >>> di_over("01/01/2025")  # Holiday
        nan
    """
    if has_nullable_args(date):
        return float("nan")

    try:
        # Convert date to expected file format: YYYYMMDD.txt
        date_obj = convert_dates(date)
        filename = date_obj.strftime("%Y%m%d.txt")

        # Use context manager for safe resource handling (auto-close/quit)
        with ftplib.FTP("ftp.cetip.com.br", timeout=10) as ftp:
            ftp.login()
            ftp.cwd("/MediaCDI")

            lines = []
            try:
                ftp.retrlines(f"RETR {filename}", lines.append)
            except ftplib.error_perm as e:
                # Code 550 usually means "File not found" (weekend/holiday/future)
                if str(e).startswith("550"):
                    logger.warning(f"DI Rate file not found for {date}: {e}")
                    return float("nan")  # Return NaN instead of raising exception

                # If it's another type of error, we raise it to be caught below
                raise e

            if not lines:
                logger.error(f"File {filename} is empty.")
                return float("nan")

            # Parse the rate
            # Format usually: "00001315" -> 13.15% -> 0.1315
            raw_rate = lines[0].strip()
            rate = int(raw_rate) / 10000
            return round(rate, DI_OVER_DECIMAL_PLACES)

    except ValueError as e:
        logger.error(f"Date format error for input '{date}': {e}")
        raise

    except ftplib.all_errors as e:
        logger.error(f"FTP connection or transfer error: {e}")
        raise ConnectionError(f"Failed to fetch DI rate from FTP: {e}") from e
