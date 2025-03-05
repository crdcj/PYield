import ftplib

from pyield.date_converter import DateScalar, convert_input_dates


def di_over(date: DateScalar) -> float:
    """
    Gets the DI (Interbank Deposit) rate for a specific date from B3/CETIP FTP server.

    Args:
        date (str): Date in DD/MM/YYYY format

    Returns:
        float: DI rate for the specified date

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
            return round(rate, 5)
        else:
            return "Empty file or no data"

    except ValueError as e:
        return f"Date format error: {e}"
    except ftplib.error_perm as e:
        return f"File access error: {e}"
    except Exception as e:
        return f"Unexpected error: {e}"
    finally:
        # Ensure FTP connection is closed
        if ftp:
            try:
                ftp.quit()
            except:  # noqa
                pass


# Example usage
if __name__ == "__main__":
    date = "28/02/2025"
    rate = di_over(date)
    print(f"DI Rate for {date}: {rate}")
