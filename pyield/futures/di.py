import pandas as pd

from ... import bday
from ..core import _fetch_raw_df, get_expiration_date, get_old_expiration_date


def _convert_prices_to_rates(prices: pd.Series, bd: pd.Series) -> pd.Series:
    """
    Internal function to convert DI futures prices to rates.

    Args:
        prices (pd.Series): A pd.Series containing DI futures prices.
        bd (pd.Series): A serie containing the number of business days to expiration.

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


def _process_raw_df(df: pd.DataFrame, trade_date: pd.Timestamp) -> pd.DataFrame:
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
        df["ExpirationDate"] = df["ExpirationCode"].apply(get_expiration_date)

    df["BDToExpiration"] = bday.count_bdays(trade_date, df["ExpirationDate"])
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
        df[col] = df[col].replace(0, pd.NA)

    # Prior to 17/01/2002 (incluive), prices were not converted to rates
    if trade_date <= pd.Timestamp("2002-01-17"):
        df = _convert_prices_in_older_contracts(df)

    df["SettlementRate"] = _convert_prices_to_rates(
        df["SettlementPrice"], df["BDToExpiration"]
    )

    # Remove percentage in all rate columns and round to 5 decimal places since it's the
    # precision used by B3. Obs: 5 decimal places = 3 decimal places in percentage
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


def fetch_di(trade_date: pd.Timestamp, return_raw: bool = False) -> pd.DataFrame:
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
    df_raw = _fetch_raw_df(trade_date)
    if return_raw:
        return df_raw
    return _process_raw_df(df_raw, trade_date)


def fetch_df(
    trade_date: pd.Timestamp,
    return_raw: bool = False,
) -> pd.DataFrame:
    """
    Fetches DI futures data for a specified trade date from B3.

     Retrieves and processes DI futures data from B3 for a given trade date. This
     function serves as the primary method for accessing DI data, with options to
     specify the source of the data and whether to return raw data.

     Args:
        trade_date (pd.Timestamp): The trade date for which to fetch DI data.
        source_type (Literal["bmf", "b3", "b3s"], optional): Indicates the source of
            the data. Defaults to "bmf". Options include:
                - "bmf": Fetches data from the old BM&FBOVESPA website. Fastest option.
                - "b3": Fetches data from the complete Price Report (XML file) provided
                    by B3.
                - "b3s": Fetches data from the simplified Price Report (XML file)
                    provided by B3. Faster than "b3" but less detailed.
        return_raw (bool, optional): If True, returns the raw DI data without
            processing.

     Returns:
         pd.DataFrame: A DataFrame containing the DI futures data for the specified
         trade date. Format and content depend on the source_type and return_raw flag.

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

    return fetch_di(trade_date, return_raw)