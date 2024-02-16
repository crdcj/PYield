import io
import warnings

import pandas as pd
import requests

from . import br_calendar as brc
from . import di_futures as dif


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
        return brc.offset_bdays(expiration, offset=0, holiday_list=brc.OLD_BR_HOLIDAYS)

    except (KeyError, ValueError):
        return pd.NaT


def get_raw_di(reference_date: str | pd.Timestamp) -> pd.DataFrame:
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
            f"A {type(e).__name__} occurred while reading the DI futures data for {reference_date.strftime('%d/%m/%Y')}. Returning an empty DataFrame."
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


def process_di(df: pd.DataFrame, reference_date: pd.Timestamp) -> pd.DataFrame:
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

    df.insert(0, "reference_date", reference_date)
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["reference_date"] = df["reference_date"].astype("datetime64[ns]")

    # Contract code format was changed in 22/05/2006
    if reference_date < pd.Timestamp("2006-05-22"):
        df["expiration"] = df["contract_code"].apply(
            get_old_expiration_date, args=(reference_date,)
        )
    else:
        df["expiration"] = df["contract_code"].apply(dif.get_expiration_date)

    df["bdays"] = brc.count_bdays(reference_date, df["expiration"])
    # Convert to nullable integer, since other columns use this data type
    df["bdays"] = df["bdays"].astype(pd.Int64Dtype())
    # Remove expired contracts
    df.query("bdays > 0", inplace=True)

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
            "reference_date",
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


def get_di(
    reference_date: str | pd.Timestamp, return_raw: bool = False
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
    df_raw = get_raw_di(reference_date)
    if return_raw:
        return df_raw
    return process_di(df_raw, reference_date)
