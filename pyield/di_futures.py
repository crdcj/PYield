import io
import warnings

import pandas as pd
import requests

from . import br_calendar as bc


def get_old_expiration_date(
    contract_code: str, reference_date: pd.Timestamp
) -> pd.Timestamp:
    """
    Internal function to convert an old DI contract code into its expiration date. Valid for
    contract codes up to 21-05-2006.

    Args:
        contract_code (str):
            An old DI contract code from B3, where the first three letters represent
            the month and the last digit represents the year. Example: "JAN3".
        reference_date (pd.Timestamp):
            The reference date for which the contract code is valid.

    Returns:
        pd.Timestamp
            The contract's expiration date.
            Returns pd.NaT if the input is invalid.

    Examples:
        >>> get_old_expiration_date("JAN3", pd.Timestamp("2001-05-21"))
        pd.Timestamp('2003-01-01')
    Notes:
        - In 22-05-2006, B3 changed the format of the DI contract codes. Before that date,
          the first three letters represented the month and the last digit represented the
          year.
    """

    month_codes = {
        "JAN": 1,
        "FEV": 2,
        "MAR": 3,
        "ABR": 4,
        "MAI": 5,
        "JUN": 6,
        "JUL": 7,
        "AGO": 8,
        "SET": 9,
        "OUT": 10,
        "NOV": 11,
        "DEZ": 12,
    }
    try:
        month_code = contract_code[:3]
        month = month_codes[month_code]

        # Year codes must generated dynamically, since it depends on the reference date
        reference_year = reference_date.year
        year_codes = {}
        for year in range(reference_year, reference_year + 10):
            year_codes[str(year)[-1:]] = year
        year = year_codes[contract_code[-1:]]

        expiration = pd.Timestamp(year, month, 1)
        # Adjust to the next business day when expiration date is a weekend or a holiday
        # Must use the old holiday calendar, since this type of contract code was used until 2006
        return bc.offset_bdays(expiration, offset=0, holiday_list=bc.OLD_BR_HOLIDAYS)

    except (KeyError, ValueError):
        return pd.NaT


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


def get_raw_di_data(reference_date: str | pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to fetch raw DI futures data from B3 for a specific reference date.

    Args:
        reference_date: a datetime-like object representing the reference date.

    Returns:
        pd.DataFrame: Raw data as a Pandas DataFrame.
    """
    reference_date = pd.Timestamp(reference_date)
    # url example: https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data=05/10/2023&Mercadoria=DI1
    url = f"https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data={reference_date.strftime('%d/%m/%Y')}&Mercadoria=DI1&XLS=false"
    r = requests.get(url)
    f = io.StringIO(r.text)

    try:
        # Attempt to get the first table with the header "AJUSTE"
        df = pd.read_html(
            f,
            match="AJUSTE",
            header=1,
            thousands=".",
            decimal=",",
            na_values=["-"],
            dtype_backend="numpy_nullable",
        )[0]

        # Remove rows with all NaN values
        df = df.dropna(how="all")
        # Remove columns with all NaN values
        df = df.dropna(axis=1, how="all")

        # Force "VAR. PTOS." column to be string, since it can vary between str and float
        df["VAR. PTOS."] = df["VAR. PTOS."].astype(pd.StringDtype())
        # Force "AJUSTE CORRIG. (4)" column to be float, since it can vary between int and float
        df["AJUSTE CORRIG. (4)"] = df["AJUSTE CORRIG. (4)"].astype(pd.Float64Dtype())

        return df

    except Exception as e:
        warnings.warn(
            f"A {type(e).__name__} occurred while reading the DI futures data for {reference_date.strftime("%d/%m/%Y")}. Returning an empty DataFrame."
        )
        return pd.DataFrame()


def convert_prices_to_rates(prices: pd.Series, bd: pd.Series) -> pd.Series:
    """
    Internal function to convert DI futures prices to rates.

    Args:
        prices (pd.Series): A Series containing DI futures prices.
        bd (pd.Series): A Series containing the number of business days to expiration.

    Returns:
        pd.Series: A Series containing DI futures rates.
    """
    rates = (100_000 / prices) ** (252 / bd) - 1
    # Return rates as percentage
    return 100 * rates


def convert_prices_in_older_contracts(df: pd.DataFrame) -> pd.DataFrame:
    # Prior to 01/01/2002, prices were not converted to rates
    convert_cols = [
        "opening_rate",
        "min_rate",
        "max_rate",
        "avg_rate",
        "closing_rate",
        "last_bid_rate",
        "last_offer_rate",
    ]
    for col in convert_cols:
        df[col] = convert_prices_to_rates(df[col], df["bdays"])
    # Invert low and high prices
    df["min_rate"], df["max_rate"] = df["max_rate"], df["min_rate"]

    return df


def process_di_data(df: pd.DataFrame, reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to process and transform raw DI futures data.

    Args:
        df (pd.DataFrame): the raw DI DataFrame.
        reference_date: a datetime-like object representing the reference date.

    Returns:
        pd.DataFrame: Processed and transformed data as a Pandas DataFrame.
    """
    # Check if the DataFrame is empty
    if df.empty:
        return df

    df = df.rename(
        columns={
            "VENCTO": "contract_code",
            "CONTR. ABERT.(1)": "open_contracts",
            "CONTR. FECH.(2)": "closed_contracts",
            "NÚM. NEGOC.": "number_of_trades",
            "CONTR. NEGOC.": "trading_volume",
            "VOL.": "financial_volume",
            "AJUSTE ANTER. (3)": "prev_settlement_price",
            "AJUSTE CORRIG. (4)": "adj_prev_settlement_price",
            "PREÇO ABERTU.": "opening_rate",
            "PREÇO MÍN.": "min_rate",
            "PREÇO MÁX.": "max_rate",
            "PREÇO MÉD.": "avg_rate",
            "ÚLT. PREÇO": "closing_rate",
            "AJUSTE": "settlement_price",
            "VAR. PTOS.": "point_variation",
            "ÚLT.OF. COMPRA": "last_bid_rate",
            "ÚLT.OF. VENDA": "last_offer_rate",
        }
    )
    reference_date = pd.Timestamp(reference_date)
    # Contract code format was changed in 22/05/2006
    if reference_date < pd.Timestamp("2006-05-22"):
        df["expiration"] = df["contract_code"].apply(
            get_old_expiration_date, args=(reference_date,)
        )
    else:
        df["expiration"] = df["contract_code"].apply(get_expiration_date)

    df["bdays"] = bc.count_bdays(reference_date, df["expiration"])
    # Convert to nullable integer, since other columns use this data type
    df["bdays"] = df["bdays"].astype(pd.Int64Dtype())
    # Remove expired contracts
    df = df[df["bdays"] > 0]

    # Columns where 0 means NaN
    cols_with_nan = [
        "settlement_price",
        "opening_rate",
        "min_rate",
        "max_rate",
        "avg_rate",
        "closing_rate",
        "last_bid_rate",
        "last_offer_rate",
    ]
    for col in cols_with_nan:
        df[cols_with_nan] = df[cols_with_nan].replace(0, pd.NA)

    # Prior to 17/01/2002 (incluive), prices were not converted to rates
    if reference_date <= pd.Timestamp("2002-01-17"):
        df = convert_prices_in_older_contracts(df)

    df["settlement_rate"] = convert_prices_to_rates(df["settlement_price"], df["bdays"])

    # Remove percentage in all rate columns and round to 5 decimal places since it's the precision used by B3
    # Obs: 5 decimal places = 3 decimal places in percentage
    rate_cols = [col for col in df.columns if "rate" in col]
    for col in rate_cols:
        df[col] = (df[col] / 100).round(5)

    # Order columns
    df = df[
        [
            "contract_code",
            "expiration",
            "bdays",
            "open_contracts",
            "closed_contracts",
            "number_of_trades",
            "trading_volume",
            "financial_volume",
            "settlement_price",
            "settlement_rate",
            "opening_rate",
            "min_rate",
            "max_rate",
            "avg_rate",
            "closing_rate",
            "last_bid_rate",
            "last_offer_rate",
        ]
    ]
    return df


def get_di_data(reference_date: str | pd.Timestamp, raw: bool = False) -> pd.DataFrame:
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
        >>> get_di_data("2023-12-28")
    """
    reference_date = pd.Timestamp(reference_date)
    df = get_raw_di_data(reference_date)
    if raw:
        return df
    df = process_di_data(df, reference_date)

    return df
