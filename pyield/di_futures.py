from typing import Literal
from pathlib import Path

import pandas as pd
from pandas import DataFrame, Timestamp

from . import di_web as diw
from . import di_xml as dix
from . import br_calendar as brc


def normalize_date(trade_date: str | Timestamp | None = None) -> Timestamp:
    if isinstance(trade_date, str):
        normalized_date = pd.Timestamp(trade_date).normalize()
    elif isinstance(trade_date, Timestamp):
        normalized_date = trade_date.normalize()
    elif trade_date is None:
        today = pd.Timestamp.today().normalize()
        normalized_date = brc.offset_bdays(today, -1)
    else:
        raise ValueError("Invalid date format.")

    # Raise an error if the trade date is in the future
    if normalized_date > pd.Timestamp.today().normalize():
        raise ValueError("Trade date cannot be in the future.")

    # Raise error if the reference date is not a business day
    if not brc.is_business_day(normalized_date):
        raise ValueError("Trade date must be a business day.")

    return normalized_date


def get_expiration_date(expiration_code: str) -> Timestamp:
    """
    Internal function to convert the expiration code into its expiration date.

    Given a expiration code, this function determines its expiration date.
    If the expiration code does not correspond to a valid month or year, or if the input
    is not in the expected format, the function will return a pd.NaT (Not a Timestamp).
    Valid for contract codes from 22-05-2006 (inclusive) onwards.

    Args:
        expiration_code (str):
            A string where the first letter represents the month and the last two digits
            represent the year. Example: "F23".

    Returns:
        pd.Timestamp
            The contract's expiration date, adjusted for a valid business day.
            Returns pd.NaT if the input is invalid.

    Examples:
        >>> get_expiration_date("F23")
        pd.Timestamp('2023-01-01')

        >>> get_expiration_date("Z33")
        pd.Timestamp('2033-12-01')

        >>> get_expiration_date("A99")
        pd.NaT

    Notes:
        - In 22-05-2006, B3 changed the format of the DI contract codes.
        - The first letter represents the month and the last two digits represent the
          year.
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
        adj_expiration = brc.offset_bdays(expiration, offset=0)

        return adj_expiration

    except (KeyError, ValueError):
        raise ValueError("Invalid expiration code.")


def get_di(
    trade_date: str | Timestamp | None = None,
    source_type: Literal["bmf", "b3", "b3s"] = "bmf",
    return_raw: bool = False,
) -> DataFrame:
    """
    Gets the DI futures data for a given date from B3.

    This function fetches and processes the DI futures data from B3 for a specific
    trade date. It's the primary external interface for accessing DI data.

    Args:
        trade_date: a datetime-like object representing the trade date. if not provided,
            the function will use the previous business day.
        source_type: a string indicating the source of the data. Options are "bmf",
            "b3" and "b3s" (default is "bmf").
            - "bmf" (fast) fetches DI data from old BM&FBOVESPA website. This is the default
              option.
            - "b3" (slow) fetches DI data from the complete Price Report (XML file) provided by B3.
            - "b3s" (fast) fetches DI data from the simplified Price Report (XML file) provided by B3.
              This option is faster than "b3" but it has less information.
        return_raw: a boolean indicating whether to return the raw DI data.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the DI Futures data for
        the given trade date.

    Examples:
        # Get the DI data for the previous business day
        >>> get_di()
        # Get the DI data for a specific date in ISO format (YYYY-MM-DD)
        >>> get_di("2023-12-28")

    Notes:
        - The Complete Price Report XML files are approximately 5 MB zipped files.
        - The Simplified Price Report XML files are approximately 50 kB zipped files.
        - File information can be found at: https://www.b3.com.br/data/files/16/70/29/9C/6219D710C8F297D7AC094EA8/Catalogo_precos_v1.3.pdf

    """
    # Force trade_date to be a pandas Timestamp
    normalized_trade_date = normalize_date(trade_date)

    if source_type == "bmf":
        return diw.get_di(normalized_trade_date, return_raw)
    elif source_type in ["b3", "b3s"]:
        return dix.get_di(normalized_trade_date, source_type, return_raw)
    else:
        raise ValueError("source_type must be either 'bmf', 'b3' or 'b3s'.")


def read_di(file_path: Path, return_raw: bool = False) -> DataFrame:
    """
    Reads a DI futures data file and returns a DataFrame.

    This function reads a DI futures data file and returns a DataFrame with the data.

    Args:
        file_path: a Path object indicating the path to the file.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the DI Futures data.

    Examples:
        >>> read_di_from_file(Path("path/to/file.xml"))
    """
    return dix.read_di(file_path, return_raw=return_raw)
