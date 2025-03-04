import pandas as pd

from pyield import bday


def get_expiration_date(expiration_code: str, expiration_day: int = 1) -> pd.Timestamp:
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
        Timestamp('2023-01-02 00:00:00')

        >>> get_expiration_date("Z33")
        Timestamp('2033-12-01 00:00:00')

        >>> get_expiration_date("A99")
        NaT

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
        # The expiration day is normally the first business day of the month
        expiration = pd.Timestamp(year, month, expiration_day)

        # Adjust to the next business day when expiration date is not a business day
        adj_expiration = bday.offset(dates=expiration, offset=0)

        return adj_expiration

    except (KeyError, ValueError):
        return pd.NaT  # type: ignore
