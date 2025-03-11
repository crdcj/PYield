import ftplib
import logging

from pyield.date_converter import DateScalar, convert_input_dates

logger = logging.getLogger(__name__)

# 4 decimal places in rate = 2 decimal places in percentage
DI_OVER_DECIMAL_PLACES = 4


def di_over(date: DateScalar) -> float:
    """
    Gets the DI (Interbank Deposit) rate for a specific date from B3/CETIP FTP server.

    Args:
        date (str): Date in DD/MM/YYYY format

    Returns:
        float: DI rate for the specified date or NaN (Not a Number) if no data is found.

    Raises:
        ValueError: If date is not in the correct format
        ftplib.error_perm: If the file is not found or there is a permission error
        Exception: For any other unexpected error

    Examples:
        >>> di_over("28/02/2025")
        0.1315
    """
    ftp = None
    try:
        # Convert date to file format
        date_obj = convert_input_dates(date)
        filename = date_obj.strftime("%Y%m%d.txt")

        # Connect to FTP and get the file
        ftp = ftplib.FTP("ftp.cetip.com.br")
        ftp.login()
        ftp.cwd("/MediaCDI")

        # Read rate
        lines = []
        ftp.retrlines(f"RETR {filename}", lines.append)

        # Close FTP connection
        ftp.quit()

        # Get and format rate
        if lines:
            raw_rate = lines[0].strip()
            rate = int(raw_rate) / 10000
            return round(rate, DI_OVER_DECIMAL_PLACES)
        else:
            logger.error(f"No data found for date {date}")
            return float("nan")

    except ValueError as e:
        logger.error(f"Date format error: {e}")
        raise
    except ftplib.error_perm as e:
        logger.error(f"File access error: {e}")
        raise
    except Exception:
        logger.exception("Unexpected error")
        raise
    finally:
        if ftp and ftp.sock:  # Ensure FTP connection is closed AND socket exists
            try:
                ftp.quit()
            except:  # noqa
                pass
