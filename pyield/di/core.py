from typing import Literal
from pathlib import Path

import pandas as pd

from . import web
from . import xml
from .. import calendar as cl


def _normalize_date(trade_date: str | pd.Timestamp | None = None) -> pd.Timestamp:
    if isinstance(trade_date, str):
        normalized_date = pd.Timestamp(trade_date).normalize()
    elif isinstance(trade_date, pd.Timestamp):
        normalized_date = trade_date.normalize()
    elif trade_date is None:
        today = pd.Timestamp.today().normalize()
        # Get last business day before today
        normalized_date = cl.offset_bdays(today, -1)
    else:
        raise ValueError("Invalid date format.")

    # Raise an error if the trade date is in the future
    if normalized_date > pd.Timestamp.today().normalize():
        raise ValueError("Trade date cannot be in the future.")

    # Raise error if the reference date is not a business day
    if not cl.is_bday(normalized_date):
        raise ValueError("Trade date must be a business day.")

    return normalized_date


def get_expiration_date(expiration_code: str) -> pd.Timestamp:
    """
    Converts an expiration code into its corresponding expiration date.

    This function translates an expiration code into a specific expiration date based on
    a given mapping. The expiration code consists of a letter representing the month and
    two digits for the year. The function ensures the date returned is a valid business
    day by adjusting weekends and holidays as necessary.

    Args:
        expiration_code (str): The expiration code to be converted, where the first
            letter represents the month and the last two digits represent the year
            (e.g., "F23" for January 2023).

    Returns:
        pd.Timestamp: The expiration date corresponding to the code, adjusted to a valid
            business day. Returns pd.NaT if the code is invalid.

    Examples:
        >>> get_expiration_date("F23")
        pd.Timestamp('2023-01-01')

        >>> get_expiration_date("Z33")
        pd.Timestamp('2033-12-01')

        >>> get_expiration_date("A99")
        pd.NaT

    Notes:
        The expiration date is calculated based on the format change introduced by B3 on
        22-05-2006, where the first letter represents the month and the last two digits
        represent the year.
    """
    month_codes = {
        "F": 1,
        "G": 2,
        "H": 3,
        "J": 4,
        "K": 5,
        "M": 6,
        "N": 7,
        "Q": 8,
        "U": 9,
        "V": 10,
        "X": 11,
        "Z": 12,
    }

    try:
        month_code = expiration_code[0]
        month = month_codes[month_code]
        year = int("20" + expiration_code[-2:])
        # The expiration date is always the first business day of the month
        expiration = pd.Timestamp(year, month, 1)

        # Adjust to the next business day when expiration date is a weekend or a holiday
        adj_expiration = cl.offset_bdays(expiration, offset=0)

        return adj_expiration

    except (KeyError, ValueError):
        raise ValueError("Invalid expiration code.")


def get_di(
    trade_date: str | pd.Timestamp | None = None,
    source_type: Literal["bmf", "b3", "b3s"] = "bmf",
    return_raw: bool = False,
) -> pd.DataFrame:
    """
    Fetches DI futures data for a specified trade date from B3.

     Retrieves and processes DI futures data from B3 for a given trade date. This function
     serves as the primary method for accessing DI data, with options to specify the source
     of the data and whether to return raw data.

     Args:
         trade_date (str | pd.Timestamp | None, optional): The trade date for which to
             fetch DI data. If None or not provided, uses the previous business day.
         source_type (Literal["bmf", "b3", "b3s"], optional): Indicates the source of the
             data. Options include:
             - "bmf": Fetches data from the old BM&FBOVESPA website. Fastest option.
             - "b3": Fetches data from the complete Price Report (XML file) provided by B3.
             - "b3s": Fetches data from the simplified Price Report (XML file) provided by B3.
               Faster than "b3" but less detailed.
             Defaults to "bmf".
         return_raw (bool, optional): If True, returns the raw DI data without processing.

     Returns:
         pd.DataFrame: A DataFrame containing the DI futures data for the specified trade
             date. Format and content depend on the source_type and return_raw flag.

     Examples:
         # Fetch DI data for the previous business day using default settings
         >>> get_di()

         # Fetch DI data for a specific trade date from the simplified B3 Price Report
         >>> get_di("2023-12-28", source_type="b3s")

     Notes:
         - Complete Price Report XML files are about 5 MB in size.
         - Simplified Price Report XML files are significantly smaller, around 50 kB.
         - For file specifications, refer to the B3 documentation: [B3 File Specs](https://www.b3.com.br/data/files/16/70/29/9C/6219D710C8F297D7AC094EA8/Catalogo_precos_v1.3.pdf)
    """
    # Force trade_date to be a pandas pd.Timestamp
    normalized_trade_date = _normalize_date(trade_date)

    if source_type == "bmf":
        return web.get_di(normalized_trade_date, return_raw)
    elif source_type in ["b3", "b3s"]:
        return xml.get_di(normalized_trade_date, source_type, return_raw)
    else:
        raise ValueError("source_type must be either 'bmf', 'b3' or 'b3s'.")


def read_di(file_path: Path, return_raw: bool = False) -> pd.DataFrame:
    """
    Reads DI futures data from a file and returns it as a pandas DataFrame.

    This function opens and reads a DI futures data file, returning the contents as a
    pandas DataFrame. It supports reading from both XML files provided by B3, wich
    are the simplified and complete Price Reports.

    Args:
        file_path (Path): The file path to the DI data file. This should be a valid
            Path object pointing to the location of the file.
        return_raw (bool, optional): If set to True, the function returns the raw data
            without applying any transformation or processing. Useful for cases where
            raw data inspection or custom processing is needed. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the processed or raw DI futures data,
            depending on the `return_raw` flag.

    Examples:
        >>> read_di(Path("path/to/di_data_file.xml"))
        # returns a DataFrame with the DI futures data

        >>> read_di(Path("path/to/di_data_file.xml"), return_raw=True)
        # returns a DataFrame with the raw DI futures data, without processing

    Note:
        The ability to process and return raw data is primarily intended for advanced
        users who require access to the data in its original form for custom analyses.
    """
    return xml.read_di(file_path, return_raw=return_raw)
