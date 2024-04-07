import io
import warnings
from typing import Optional

import pandas as pd
import requests

from .. import calendar as cl
from . import core as cr


def get_old_expiration_date(
    ExpirationCode: str, trade_date: pd.Timestamp
) -> Optional[pd.Timestamp]:
    """
    Internal function to convert an old DI contract code into its ExpirationDate date. Valid for
    contract codes up to 21-05-2006.

    Args:
        ExpirationCode (str):
            An old DI ExpirationCode from B3, where the first three letters represent
            the month and the last digit represents the year. Example: "JAN3".
        trade_date (pd.Timestamp):
            The trade date for which the contract code is valid.

    Returns:
        pd.Timestamp
            The contract's ExpirationDate date.
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
        month_code = ExpirationCode[:3]
        month = month_codes[month_code]

        # Year codes must generated dynamically, since it depends on the trade date
        reference_year = trade_date.year
        year_codes = {}
        for year in range(reference_year, reference_year + 10):
            year_codes[str(year)[-1:]] = year
        year = year_codes[ExpirationCode[-1:]]

        ExpirationDate = pd.Timestamp(year, month, 1)
        # Adjust to the next business day when ExpirationDate date is a weekend or a holiday
        # Must use the old holiday calendar, since this type of contract code was used until 2006
        return cl.offset_bdays(ExpirationDate, offset=0, holiday_list="old")

    except (KeyError, ValueError):
        return pd.NaT  # type: ignore


def _get_raw_di(trade_date: pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to fetch raw DI futures data from B3 for a specific trade date.

    Args:
        trade_date: a datetime-like object representing the trade date.

    Returns:
        pd.DataFrame: Raw DI data as a Pandas pd.DataFrame.
    """
    # url example: https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data=05/10/2023&Mercadoria=DI1
    url = f"https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data={trade_date.strftime('%d/%m/%Y')}&Mercadoria=DI1&XLS=false"
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
            f"A {type(e).__name__} occurred while reading the DI futures data for {trade_date.strftime('%d/%m/%Y')}. Returning an empty pd.DataFrame."
        )
        return pd.DataFrame()


def _convert_prices_to_rates(prices: pd.Series, bd: pd.Series) -> pd.Series:
    """
    Internal function to convert DI futures prices to rates.

    Args:
        prices (pd.Series): A pd.Series containing DI futures prices.
        bd (pd.Series): A pd.Series containing the number of business days to ExpirationDate.

    Returns:
        pd.Series: A pd.Series containing DI futures rates.
    """
    rates = (100_000 / prices) ** (252 / bd) - 1

    # Return rates as percentage
    return 100 * rates


def _convert_prices_in_older_contracts(df: pd.DataFrame) -> pd.DataFrame:
    # Prior to 01/01/2002, prices were not converted to rates
    convert_cols = [
        "FirstRate",
        "MinRate",
        "MaxRate",
        "AvgRate",
        "LastRate",
        "LastBidRate",
        "LastAskRate",
    ]
    for col in convert_cols:
        df[col] = _convert_prices_to_rates(df[col], df["BDToExpiration"])

    # Invert low and high prices
    df["MinRate"], df["MaxRate"] = df["MaxRate"], df["MinRate"]

    return df


def _process_di(df: pd.DataFrame, trade_date: pd.Timestamp) -> pd.DataFrame:
    """
    Internal function to process and transform raw DI futures data.

    Args:
        df (pd.DataFrame): the raw DI DataFrame.
        trade_date: a datetime-like object representing the trade date.

    Returns:
        pd.DataFrame: Processed and transformed data as a Pandas pd.DataFrame.
    """
    # Check if the pd.DataFrame is empty
    if df.empty:
        return df

    rename_dict = {
        "VENCTO": "ExpirationCode",
        "CONTR. ABERT.(1)": "OpenContracts",  # At the start of the day
        "CONTR. FECH.(2)": "OpenContractsEndSession",  # At the end of the day
        "NÚM. NEGOC.": "NumOfTrades",
        "CONTR. NEGOC.": "TradedQuantity",
        "VOL.": "FinancialVolume",
        "AJUSTE": "SettlementPrice",
        "AJUSTE ANTER. (3)": "PrevSettlementRate",
        "AJUSTE CORRIG. (4)": "AdjSettlementRate",
        "PREÇO MÍN.": "MinRate",
        "PREÇO MÉD.": "AvgRate",
        "PREÇO MÁX.": "MaxRate",
        "PREÇO ABERTU.": "FirstRate",
        "ÚLT. PREÇO": "LastRate",
        "VAR. PTOS.": "PointsVariation",
        # Attention: bid/ask rates are inverted
        "ÚLT.OF. COMPRA": "LastAskRate",
        "ÚLT.OF. VENDA": "LastBidRate",
    }

    df = df.rename(columns=rename_dict)

    df["TradeDate"] = trade_date
    # Convert to datetime64[ns] since it is pandas default type for timestamps
    df["TradeDate"] = df["TradeDate"].astype("datetime64[ns]")

    # Contract code format was changed in 22/05/2006
    if trade_date < pd.Timestamp("2006-05-22"):
        df["ExpirationDate"] = df["ExpirationCode"].apply(
            get_old_expiration_date, args=(trade_date,)
        )
    else:
        df["ExpirationDate"] = df["ExpirationCode"].apply(cr.get_expiration_date)

    df["BDToExpiration"] = cl.count_bdays(trade_date, df["ExpirationDate"])
    # Convert to nullable integer, since other columns use this data type
    df["BDToExpiration"] = df["BDToExpiration"].astype(pd.Int64Dtype())
    # Remove expired contracts
    df.query("BDToExpiration > 0", inplace=True)

    # Columns where 0 means NaN
    cols_with_nan = [
        "SettlementPrice",
        "FirstRate",
        "MinRate",
        "MaxRate",
        "AvgRate",
        "LastRate",
        "LastBidRate",
        "LastAskRate",
    ]
    for col in cols_with_nan:
        df[cols_with_nan] = df[cols_with_nan].replace(0, pd.NA)

    # Prior to 17/01/2002 (incluive), prices were not converted to rates
    if trade_date <= pd.Timestamp("2002-01-17"):
        df = _convert_prices_in_older_contracts(df)

    df["SettlementRate"] = _convert_prices_to_rates(
        df["SettlementPrice"], df["BDToExpiration"]
    )

    # Remove percentage in all rate columns and round to 5 decimal places since it's the precision used by B3
    # Obs: 5 decimal places = 3 decimal places in percentage
    rate_cols = [col for col in df.columns if "Rate" in col]
    for col in rate_cols:
        df[col] = (df[col] / 100).round(5)

    # Filter and order columns
    ordered_cols = [
        "TradeDate",
        "ExpirationCode",
        "ExpirationDate",
        "BDToExpiration",
        "OpenContracts",
        # "OpenContractsEndSession" since there is no OpenContracts at the end of the
        # day in XML data, it will be removed to avoid confusion with XML data
        "NumOfTrades",
        "TradedQuantity",
        "FinancialVolume",
        "SettlementPrice",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "FirstRate",
        "LastRate",
        "LastAskRate",
        "LastBidRate",
        "SettlementRate",
    ]
    return df[ordered_cols]


def get_di(trade_date: pd.Timestamp, return_raw: bool = False) -> pd.DataFrame:
    """
    Gets the DI futures data for a given date from B3.

    This function fetches and processes the DI futures data from B3 for a specific
    trade date. It's the primary external interface for accessing DI data.

    Args:
        trade_date: a datetime-like object representing the trade date.
        raw (bool): If True, returns the raw data as a Pandas pd.DataFrame.
            Defaults to False.

    Returns:
        pd.DataFrame: A Pandas pd.DataFrame containing processed DI futures data.

    Examples:
        >>> get_di("2023-12-28")

    Notes:
        - BDToExpiration: number of business days to ExpirationDate.
        - OpenContracts: number of open contracts at the start of the trading day.
        - closed_contracts: number of closed contracts at the end of the trading day.
    """
    df_raw = _get_raw_di(trade_date)
    if return_raw:
        return df_raw
    return _process_di(df_raw, trade_date)
