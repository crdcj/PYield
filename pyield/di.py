import io
import warnings
import requests
import pandas as pd
import numpy as np

from . import bd_calculator as wd


def convert_old_contract_code(
    contract_code: str, reference_date: pd.Timestamp
) -> pd.Timestamp:
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
    month_code = contract_code[:3]
    month = month_codes[month_code]

    base_year = reference_date.year
    year_codes = {}
    for year in range(base_year, base_year + 10):
        year_codes[str(year)[-1:]] = year

    year = year_codes[contract_code[-1:]]
    return pd.Timestamp(year, month, 1)


def convert_contract_code(contract_code: str) -> pd.Timestamp:
    """
    Internal function to convert a DI contract code into its maturity date.

    Given a DI contract code from B3, this function determines its maturity date.
    If the contract code does not correspond to a valid month or year, or if the input
    is not in the expected format, the function will return a pd.NaT (Not a Timestamp).

    Args:
        contract_code (str):
            A DI contract code from B3, where the first letter represents the month
            and the last two digits represent the year. Example: "F23".

    Returns:
        pd.Timestamp
            The contract's maturity date, adjusted to the next business day.
            Returns pd.NaT if the input is invalid.

    Examples:
        >>> convert_contract_code("F23")
        pd.Timestamp('2023-01-01')

        >>> convert_contract_code("Z33")
        pd.Timestamp('2033-12-01')

        >>> convert_contract_code("A99")
        pd.NaT

    Notes:
        If the contract code does not represent a valid month or if the year is not in
        the range [2000, 2099], a ValueError will be raised and the function will return
        pd.NaT.

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
        return pd.Timestamp(year, month, 1)

    except ValueError:
        return pd.NaT


def get_raw_di_data(reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to fetch raw DI futures data from B3 for a specific reference date.

    Args:
        reference_date (pd.Timestamp): The reference date for which data is to be fetched.

    Returns:
        pd.DataFrame: Raw data as a Pandas DataFrame.
    """
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
            dtype_backend="numpy_nullable",
        )[0]
        # Remove rows with all NaN values
        df = df.dropna(how="all")
        # Remove columns with all NaN values
        df = df.dropna(axis=1, how="all")
        return df

    except Exception as e:
        warnings.warn(
            f"A {type(e).__name__} occurred while reading the DI futures data for {reference_date.strftime("%d/%m/%Y")}. Is this a valid date? Returning an empty DataFrame."
        )
        return pd.DataFrame()


def convert_prices_to_rates(prices: pd.Series, bd: pd.Series) -> pd.Series:
    """
    Internal function to convert DI futures prices to rates.

    Args:
        prices (pd.Series): A Series containing DI futures prices.
        bd (pd.Series): A Series containing the number of business days to maturity.

    Returns:
        pd.Series: A Series containing DI futures rates.
    """
    rates = (100_000 / prices) ** (252 / bd) - 1
    # Convert to percentage and round to 3 decimal places
    return (100 * rates).round(3)


def process_di_data(df: pd.DataFrame, reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to process and transform raw DI futures data.

    Args:
        df (pd.DataFrame): The raw data DataFrame.
        reference_date (pd.Timestamp): The reference date for data processing.

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
            "ÚLT.OF. COMPRA": "last_bid",
            "ÚLT.OF. VENDA": "last_offer",
        }
    )
            
    # Contract code format was changed in 22/05/2006
    if reference_date < pd.Timestamp("2006-05-22"):
        df["maturity"] = df["contract_code"].apply(
            convert_old_contract_code, args=(reference_date,)
        )
    else:
        df["maturity"] = df["contract_code"].apply(convert_contract_code)

    df["bdays"] = wd.count_business_days(reference_date, df["maturity"])
    # Previous settlement rates have one more business day
    df["prev_bd"] = df["bdays"] + 1

    # Remove rows with bday <= 0
    df = df[df["bdays"] > 0]
    
    # Column "adj_prev_settlement_price" can contain "-" values. Convert them to NaN.
    df["adj_prev_settlement_price"] = (df["adj_prev_settlement_price"]
        .replace("-", np.nan)
        .astype(pd.Float64Dtype())
    )

    # Columns where 0 means NaN
    cols_with_nan = [
        "prev_settlement_price",
        "adj_prev_settlement_price",
        "settlement_price",
        "opening_rate",
        "min_rate",
        "max_rate",
        "avg_rate",
        "closing_rate",
        "last_bid",
        "last_offer",
    ]
    for col in cols_with_nan:
        df[cols_with_nan] = df[cols_with_nan].replace(0, np.nan)

    df["settlement_rate"] = convert_prices_to_rates(df["settlement_price"], df["bdays"])

    # Prior to 01/01/2002, prices were not converted to rates
    convert_cols = [
        "opening_rate",
        "min_rate",
        "max_rate",
        "avg_rate",
        "closing_rate",
        "last_bid",
        "last_offer",
    ]
    if reference_date < pd.Timestamp("2002-01-01"):
        for col in convert_cols:
            df[col] = convert_prices_to_rates(df[col], df["bdays"])
        # Invert low and high prices
        df["min_rate"], df["max_rate"] = df["max_rate"], df["min_rate"]

    # Order columns
    df = df[
        [
            "contract_code",
            "maturity",
            "bdays",
            "open_contracts",
            "closed_contracts",
            "number_of_trades",
            "trading_volume",
            "financial_volume",
            "opening_rate",
            "min_rate",
            "max_rate",
            "avg_rate",
            "closing_rate",
            "last_bid",
            "last_offer",
            "settlement_rate",
        ]
    ]
    return df


def get_di_data(reference_date: str, raw=False) -> pd.DataFrame:
    """
    Gets the DI futures data for a given date from B3.

    This function fetches and processes the DI futures data from B3 for a specific
    reference date. It's the primary external interface for accessing DI data.

    Args:
        reference_date (str): The reference date in the format "dd-mm-yyyy".
        raw (bool): If True, returns the raw data as a Pandas DataFrame.
            Defaults to False.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing processed DI futures data.

    Examples:
        >>> get_di_data("25-11-2023")
    """
    reference_date = pd.to_datetime(reference_date, format="%d-%m-%Y")
    df = get_raw_di_data(reference_date)
    if raw:
        return df
    df = process_di_data(df, reference_date)

    return df
