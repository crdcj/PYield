import io

import pandas as pd
import requests

from ... import bday


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
        adj_expiration = bday.offset_bdays(expiration, offset=0)

        return adj_expiration

    except (KeyError, ValueError):
        return pd.NaT  # type: ignore


def get_old_expiration_date(
    expiration_code: str, trade_date: pd.Timestamp
) -> pd.Timestamp:
    """
    Internal function to convert an old DI contract code into its ExpirationDate date.
    Valid for contract codes up to 21-05-2006.

    Args:
        expiration_code (str): An old DI Expiration Code from B3, where the first three
            letters represent the month and the last digit represents the year.
            Example: "JAN3".
        trade_date (pd.Timestamp): The trade date for which the contract code is valid.

    Returns:
        pd.Timestamp
            The contract's ExpirationDate date. Returns pd.NaT if the input is invalid.

    Examples:
        >>> get_old_expiration_date("JAN3", pd.Timestamp("2001-05-21"))
        pd.Timestamp('2003-01-01')

    Notes:
        - In 22-05-2006, B3 changed the format of the DI contract codes. Before that
        date, the first three letters represented the month and the last digit
        represented the year.
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
        month_code = expiration_code[:3]
        month = month_codes[month_code]

        # Year codes must generated dynamically, since it depends on the trade date.
        reference_year = trade_date.year
        year_codes = {}
        for year in range(reference_year, reference_year + 10):
            year_codes[str(year)[-1:]] = year
        year = year_codes[expiration_code[-1:]]

        expiration_date = pd.Timestamp(year, month, 1)
        # Adjust to the next business day when the date is a weekend or a holiday.
        # Must use old holiday list, since this contract code was used until 2006.
        return bday.offset_bdays(expiration_date, offset=0, holiday_list="old")

    except (KeyError, ValueError):
        return pd.NaT  # type: ignore


def fetch_raw_df(asset_code: str, trade_date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the historical futures data from B3 for a specific trade date. If the data is
    not available, an empty DataFrame is returned.

    Args:
        trade_date (pd.Timestamp): The trade date for which the data should be fetched.

    Returns:
        pd.DataFrame: Raw DI data as a Pandas pd.DataFrame.
    """
    url_date = trade_date.strftime("%d/%m/%Y")
    # url example: https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data=05/10/2023&Mercadoria=DI1
    url = f"https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data={url_date}&Mercadoria={asset_code}&XLS=false"
    r = requests.get(url)

    text = r.text
    if "AJUSTE" not in text:
        return pd.DataFrame()

    df = pd.read_html(
        io.StringIO(text),
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

    # Force "VAR. PTOS." to be string, since it can also be read as float
    df["VAR. PTOS."] = df["VAR. PTOS."].astype(pd.StringDtype())

    # Force "AJUSTE CORRIG. (4)" to be float, since it can be also read as int
    if "AJUSTE CORRIG. (4)" in df.columns:
        df["AJUSTE CORRIG. (4)"] = df["AJUSTE CORRIG. (4)"].astype(pd.Float64Dtype())

    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    all_columns = {
        "VENCTO": "ExpirationCode",
        "CONTR. ABERT.(1)": "OpenContracts",  # At the start of the day
        "CONTR. FECH.(2)": "OpenContractsEndSession",  # At the end of the day
        "NÚM. NEGOC.": "TradeCount",
        "CONTR. NEGOC.": "TradeVolume",
        "VOL.": "FinancialVolume",
        "AJUSTE": "SettlementPrice",
        "AJUSTE ANTER. (3)": "PrevSettlementRate",
        "AJUSTE CORRIG. (4)": "AdjSettlementRate",
        "AJUSTE  DE REF.": "SettlementRate",  # FRC
        "PREÇO MÍN.": "MinRate",
        "PREÇO MÉD.": "AvgRate",
        "PREÇO MÁX.": "MaxRate",
        "PREÇO ABERTU.": "FirstRate",
        "ÚLT. PREÇO": "CloseRate",
        "VAR. PTOS.": "PointsVariation",
        # Attention: bid/ask rates are inverted
        "ÚLT.OF. COMPRA": "CloseAskRate",
        "ÚLT.OF. VENDA": "CloseBidRate",
    }
    rename_dict = {c: all_columns[c] for c in all_columns if c in df.columns}
    return df.rename(columns=rename_dict)


def process_raw_df(
    df: pd.DataFrame, trade_date: pd.Timestamp, asset_code: str
) -> pd.DataFrame:
    df = rename_columns(df)

    df["TradeDate"] = trade_date
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["TradeDate"] = df["TradeDate"].astype("datetime64[ns]")

    df["TickerSymbol"] = asset_code + df["ExpirationCode"]

    # Contract code format was changed in 22/05/2006
    if trade_date < pd.Timestamp("2006-05-22"):
        df["ExpirationDate"] = df["ExpirationCode"].apply(
            get_old_expiration_date, args=(trade_date,)
        )
    else:
        df["ExpirationDate"] = df["ExpirationCode"].apply(get_expiration_date)

    # Columns where 0 means NaN
    cols_with_nan = [col for col in df.columns if "Rate" in col]
    if "SettlementPrice" in df.columns:
        cols_with_nan.append("SettlementPrice")
    # Replace 0 with NaN in these columns
    df[cols_with_nan] = df[cols_with_nan].replace(0, pd.NA)

    return df


def reorder_columns(df: pd.DataFrame):
    all_columns = [
        "TradeDate",
        "TickerSymbol",
        # "ExpirationCode",
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        "OpenContracts",
        # "OpenContractsEndSession" since there is no OpenContracts at the end of the
        # day in XML data, it will be removed to avoid confusion with XML data
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "SettlementPrice",
        "SettlementRate",
        "FirstRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "CloseAskRate",
        "CloseBidRate",
        "CloseRate",
    ]
    reordered_columns = [col for col in all_columns if col in df.columns]
    return df[reordered_columns]
