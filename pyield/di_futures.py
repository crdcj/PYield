from typing import Literal
from pathlib import Path
import pandas as pd

from . import di_url
from . import di_xml
from . import br_calendar as bc


def get_expiration_date(contract_code: str) -> pd.Timestamp:
    """
    Internal function to convert a DI contract code into its expiration date.

    Given a DI contract code from B3, this function determines its expiration date.
    If the contract code does not correspond to a valid month or year, or if the input
    is not in the expected format, the function will return a pd.NaT (Not a Timestamp).
    Valid for contract codes from 22-05-2006 onwards.

    Args:
        contract_code (str):
            A DI contract code from B3, where the first letter represents the month
            and the last two digits represent the year. Example: "F23".

    Returns:
        pd.Timestamp
            The contract's expiration date, adjusted to the next business day.
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
        - Only the new holiday calendar is used, since this type of contract code
          was adopted from 2006 onwards, the expiration date is the first business day of
          the month and the new holiday calendar inserted a holiday in 20th of November.

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
        month_code = contract_code[0]
        month = month_codes[month_code]
        year = int("20" + contract_code[-2:])
        # The expiration date is always the first business day of the month
        expiration = pd.Timestamp(year, month, 1)
        # Adjust to the next business day when expiration date is a weekend or a holiday
        # Only the new holiday calendar is used, see docstring for more details
        return bc.offset_bdays(expiration, offset=0, holiday_list=bc.NEW_BR_HOLIDAYS)

    except (KeyError, ValueError):
        return pd.NaT


def get_di(
    reference_date: str | pd.Timestamp,
    return_raw: bool = False,
    source_type: Literal["xml", "html"] = "html",
    data_path: Path = None,
) -> pd.DataFrame:
    """
    Gets the DI futures data for a given date from B3.

    This function fetches and processes the DI futures data from B3 for a specific
    reference date. It's the primary external interface for accessing DI data.

    Args:
        reference_date: a datetime-like object representing the reference date.
        raw (bool): If True, returns the raw data as a Pandas DataFrame.
            Defaults to False.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing processed DI futures data.

    Examples:
        >>> get_di("2023-12-28")

    Columns:
        - bdays: number of business days to expiration.
        - open_contracts: number of open contracts at the start of the trading day.
        - closed_contracts: number of closed contracts at the end of the trading day.
    """
    reference_date = pd.Timestamp(reference_date)
    if not reference_date:
        raise ValueError("Uma data de referência válida deve ser fornecida.")

    if source_type == "xml":
        df = di_xml.get_di(reference_date, data_path, return_raw)
    elif source_type == "html":
        df = di_url.get_di(reference_date, return_raw)
    else:
        raise ValueError("source_type must be either 'xml' or 'html'.")

    return df
