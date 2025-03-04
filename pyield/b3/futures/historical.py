import io

import pandas as pd
import requests

from pyield import bday
from pyield.b3.futures import common

COUNT_CONVENTIONS = {
    "DAP": 252,
    "DI1": 252,
    "DDI": 360,
    "FRC": None,
    "DOL": None,
    "WDO": None,
    "IND": None,
    "WIN": None,
}


def get_old_expiration_date(expiration_code: str, date: pd.Timestamp) -> pd.Timestamp:
    """
    Internal function to convert an old DI contract code into its ExpirationDate date.
    Valid for contract codes up to 21-05-2006.

    Args:
        expiration_code (str): An old DI Expiration Code from B3, where the first three
            letters represent the month and the last digit represents the year.
            Example: "JAN3".
        date (pd.Timestamp): The trade date for which the contract code is valid.

    Returns:
        pd.Timestamp
            The contract's ExpirationDate date. Returns pd.NaT if the input is invalid.

    Examples:
        >>> get_old_expiration_date("JAN3", pd.Timestamp("2001-05-21"))
        Timestamp('2003-01-02 00:00:00')

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
        reference_year = date.year
        year_codes = {}
        for year in range(reference_year, reference_year + 10):
            year_codes[str(year)[-1:]] = year
        year = year_codes[expiration_code[-1:]]

        expiration_date = pd.Timestamp(year, month, 1)
        # Adjust to the next business day when the date is a weekend or a holiday.
        # Must use old holiday list, since this contract code was used until 2006.
        return bday.offset(dates=expiration_date, offset=0)

    except (KeyError, ValueError):
        return pd.NaT  # type: ignore


def _convert_prices_to_rates(
    prices: pd.Series, days_to_expiration: pd.Series, count_convention: int
) -> pd.Series:
    """
    Internal function to convert DI futures prices to rates.

    Args:
        prices (pd.Series): The futures prices to be converted.
        days_to_expiration (pd.Series): The number of days to expiration for each price.
        count_convention (int): The count convention for the DI futures contract.
            Can be 252 business days or 360 calendar days.

    Returns:
        pd.Series: A pd.Series containing the futures rates.
    """
    if count_convention == 252:  # noqa
        rates = (100_000 / prices) ** (252 / days_to_expiration) - 1
    elif count_convention == 360:  # noqa
        rates = (100_000 / prices - 1) * (360 / days_to_expiration)
    else:
        raise ValueError("Invalid count_convention. Must be 252 or 360.")

    # Round to 5 (3 in %) dec. places (contract's current max. precision)
    return rates.round(5)


def _fetch_raw_df(contract_code: str, date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the historical futures data from B3 for a specific trade date. If the data is
    not available, an empty DataFrame is returned.

    Args:
        date (pd.Timestamp): The trade date for which the data should be fetched.

    Returns:
        pd.DataFrame: Raw DI data as a Pandas pd.DataFrame.
    """
    url_date = date.strftime("%d/%m/%Y")
    # url example: https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data=05/10/2023&Mercadoria=DI1
    url = f"https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data={url_date}&Mercadoria={contract_code}&XLS=false"
    r = requests.get(url, timeout=10)

    text = r.text
    if "VENCTO" not in text:
        return pd.DataFrame()

    df = pd.read_html(
        io.StringIO(text),
        match="VENCTO",
        header=1,
        thousands=".",
        decimal=",",
        na_values=["-"],
        dtype_backend="numpy_nullable",
    )[0]

    # Remove rows and columns with all NaN values
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all").copy()

    # Force "VAR. PTOS." to be string, since it can also be read as float
    df["VAR. PTOS."] = df["VAR. PTOS."].astype("string")

    # Force "AJUSTE CORRIG. (4)" to be float, since it can be also read as int
    if "AJUSTE CORRIG. (4)" in df.columns:
        df["AJUSTE CORRIG. (4)"] = df["AJUSTE CORRIG. (4)"].astype("Float64")

    return df


def _adjust_older_contracts_rates(df: pd.DataFrame, rate_cols: list) -> pd.DataFrame:
    for col in rate_cols:
        df[col] = _convert_prices_to_rates(df[col], df["BDaysToExp"], 252)

    # Invert low and high prices
    df["MinRate"], df["MaxRate"] = df["MaxRate"], df["MinRate"]

    return df


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
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
        "PREÇO ABERTU.": "OpenRate",
        "ÚLT. PREÇO": "CloseRate",
        "VAR. PTOS.": "PointsVariation",
        # Attention: bid/ask rates are inverted
        "ÚLT.OF. COMPRA": "CloseAskRate",
        "ÚLT.OF. VENDA": "CloseBidRate",
    }
    rename_dict = {c: all_columns[c] for c in all_columns if c in df.columns}
    return df.rename(columns=rename_dict)


def process_df(
    input_df: pd.DataFrame, date: pd.Timestamp, asset_code: str
) -> pd.DataFrame:
    df = input_df.copy()
    df["TradeDate"] = date
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["TradeDate"] = df["TradeDate"].astype("datetime64[ns]")

    df["TickerSymbol"] = asset_code + df["ExpirationCode"]

    # Contract code format was changed in 22/05/2006
    if date < pd.Timestamp("2006-05-22"):
        df["ExpirationDate"] = df["ExpirationCode"].apply(
            get_old_expiration_date, args=(date,)
        )
    else:
        expiration_day = 15 if asset_code == "DAP" else 1
        df["ExpirationDate"] = df["ExpirationCode"].apply(
            common.get_expiration_date, args=(expiration_day,)
        )

    df["DaysToExp"] = (df["ExpirationDate"] - date).dt.days
    # Convert to nullable integer, since it is the default type in the library
    df["DaysToExp"] = df["DaysToExp"].astype("Int64")

    df["BDaysToExp"] = bday.count(date, df["ExpirationDate"])

    # Remove expired contracts
    df.query("DaysToExp > 0", inplace=True)

    # Columns where 0 means NaN
    cols_with_nan = [col for col in df.columns if "Rate" in col]
    if "SettlementPrice" in df.columns:
        cols_with_nan.append("SettlementPrice")
    # Replace 0 with NaN in these columns
    df[cols_with_nan] = df[cols_with_nan].replace(0, pd.NA)

    rate_cols = [col for col in df.columns if "Rate" in col]
    # Prior to 17/01/2002 (inclusive), DI prices were not converted to rates
    if date <= pd.Timestamp("2002-01-17") and asset_code == "DI1":
        df = _adjust_older_contracts_rates(df, rate_cols)
    else:
        # Remove % and round to 5 (3 in %) dec. places in rate columns
        df[rate_cols] = df[rate_cols].div(100).round(5)

    if COUNT_CONVENTIONS[asset_code] == 252:  # noqa
        df["SettlementRate"] = _convert_prices_to_rates(
            prices=df["SettlementPrice"],
            days_to_expiration=df["BDaysToExp"],
            count_convention=252,
        )
    elif COUNT_CONVENTIONS[asset_code] == 360:  # noqa
        df["SettlementRate"] = _convert_prices_to_rates(
            prices=df["SettlementPrice"],
            days_to_expiration=df["DaysToExp"],
            count_convention=360,
        )

    # Calculate DV01 for DI1 contracts
    if asset_code == "DI1":
        duration = df["BDaysToExp"] / 252
        modified_duration = duration / (1 + df["SettlementRate"])
        df["DV01"] = 0.0001 * modified_duration * df["SettlementPrice"]
        df["DV01"] = df["DV01"].astype("Float64")

    return df


def _select_and_reorder_columns(df: pd.DataFrame):
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
        "DV01",
        "SettlementPrice",
        "SettlementRate",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "CloseAskRate",
        "CloseBidRate",
        "CloseRate",
    ]
    reordered_columns = [col for col in all_columns if col in df.columns]
    return df[reordered_columns]


def fetch_historical_df(contract_code: str, date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetchs the futures data for a given date from B3.

    This function fetches and processes the futures data from B3 for a specific
    trade date. It's the primary external interface for accessing futures data.

    Args:
        asset_code (str): The asset code to fetch the futures data.
        date (pd.Timestamp): The trade date to fetch the futures data.
        count_convention (int): The count convention for the DI futures contract.
            Can be 252 business days or 360 calendar days.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing processed futures data. If
            the data is not available, an empty DataFrame is returned.
    """
    df_raw = _fetch_raw_df(contract_code, date)
    if df_raw.empty:
        return df_raw

    df = _rename_columns(df_raw)
    df = process_df(df, date, contract_code)
    df = _select_and_reorder_columns(df)
    return df
