import datetime as dt
import io
import logging

import pandas as pd
import polars as pl
import requests

from pyield import bday
from pyield.fwd import forwards
from pyield.retry import default_retry

logger = logging.getLogger(__name__)
COUNT_CONVENTIONS = {"DAP": 252, "DI1": 252, "DDI": 360}
BDAYS_PER_YEAR = 252
CDAYS_PER_YEAR = 360


def _get_expiration_date(
    expiration_code: str, expiration_day: int = 1
) -> dt.date | None:
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
        dt.date: The expiration date corresponding to the code, adjusted to a valid
            business day. Returns None if the code is invalid.

    Examples:
        >>> _get_expiration_date("F23")
        datetime.date(2023, 1, 2)

        >>> _get_expiration_date("Z33")
        datetime.date(2033, 12, 1)

        >>> _get_expiration_date("A99")

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
        expiration = dt.date(year, month, expiration_day)

        # Adjust to the next business day when expiration date is not a business day
        adj_expiration = bday.offset(dates=expiration, offset=0)

        return adj_expiration

    except (KeyError, ValueError):
        return None


def _get_old_expiration_date(date: dt.date, expiration_code: str) -> dt.date | None:
    """
    Internal function to convert an old DI contract code into its ExpirationDate date.
    Valid for contract codes up to 21-05-2006.

    Args:
        expiration_code (str): An old DI Expiration Code from B3, where the first three
            letters represent the month and the last digit represents the year.
            Example: "JAN3".
        date (dt.date): The trade date for which the contract code is valid.

    Returns:
        dt.date
            The contract's ExpirationDate date. Returns None if the input is invalid.

    Examples:
        >>> _get_old_expiration_date(dt.date(2001, 5, 21), "JAN3")
        datetime.date(2003, 1, 2)

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

        expiration_date = dt.date(year, month, 1)
        # Adjust to the next business day when the date is a weekend or a holiday.
        # Must use old holiday list, since this contract code was used until 2006.
        return bday.offset(dates=expiration_date, offset=0)

    except (KeyError, ValueError):
        return None


def _convert_prices_to_rates(
    prices: pl.Series | pd.Series,
    days_to_expiration: pl.Series | pd.Series,
    count_convention: int,
) -> pl.Series:
    """Converte preços de futuros DI em taxas usando Polars.

    Aceita Series do Polars ou Pandas e retorna sempre um `pl.Series`.
    Precisão: 5 casas (equivalente a 3 em %).
    """
    # Normaliza para polars
    if isinstance(prices, pd.Series):
        prices_pl = pl.Series(prices.name or "price", prices.to_list())
    else:
        prices_pl = prices
    if isinstance(days_to_expiration, pd.Series):
        du_pl = pl.Series(days_to_expiration.name or "du", days_to_expiration.to_list())
    else:
        du_pl = days_to_expiration

    if count_convention == BDAYS_PER_YEAR:
        rates_expr = (100_000 / prices_pl) ** (BDAYS_PER_YEAR / du_pl) - 1
    elif count_convention == CDAYS_PER_YEAR:
        rates_expr = (100_000 / prices_pl - 1) * (CDAYS_PER_YEAR / du_pl)
    else:
        raise ValueError("Invalid count_convention. Must be 252 or 360.")

    return pl.Series("rate", rates_expr).round(5)


@default_retry
def _fetch_html_data(date: dt.date, contract_code: str) -> str:
    url_date = date.strftime("%d/%m/%Y")
    # url example: https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data=05/10/2023&Mercadoria=DI1
    url_base = "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp"
    params = {"Data": url_date, "Mercadoria": contract_code, "XLS": "true"}
    r = requests.get(url_base, params=params, timeout=10)
    r.raise_for_status()
    r.encoding = "iso-8859-1"
    if "VENCTO" not in r.text:
        logger.warning(
            "No valid data found for %s on %s. Returning empty text.",
            contract_code,
            date.strftime("%d-%m-%Y"),
        )
        return ""
    return r.text


def _parse_raw_df(html_text: str) -> pl.DataFrame:
    df = pd.read_html(
        io.StringIO(html_text),
        match="VENCTO",
        header=1,
        thousands=".",
        decimal=",",
        na_values=["-"],
        dtype_backend="pyarrow",
    )[0]
    return pl.from_pandas(df)


def _pre_process_df(df: pl.DataFrame) -> pl.DataFrame:
    # Remove rows and columns where all values are null
    cols = [s.name for s in df if not (s.null_count() == df.height)]
    df = (
        df.select(cols)
        .filter(~pl.all_horizontal(pl.all().is_null()))
        .with_columns(pl.col("VAR. PTOS.").cast(pl.String))
    )
    return df


def _adjust_older_contracts_rates(df: pl.DataFrame, rate_cols: list) -> pl.DataFrame:
    """Adjust legacy DI1 contract pricing (pre-2002) converting prices -> rates."""
    for col in rate_cols:
        rate_col = _convert_prices_to_rates(df[col], df["BDaysToExp"], BDAYS_PER_YEAR)
        df = df.with_columns(col=rate_col)
    if {"MinRate", "MaxRate"}.issubset(rate_cols):
        df = (
            df.with_columns(
                pl.col("MaxRate").alias("_tmp_max"),
                pl.col("MinRate").alias("_tmp_min"),
            )
            .with_columns(
                pl.col("_tmp_max").alias("MinRate"),
                pl.col("_tmp_min").alias("MaxRate"),
            )
            .drop(["_tmp_max", "_tmp_min"])
        )
    return df


def _rename_columns(df: pl.DataFrame) -> pl.DataFrame:
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
    return df.rename(rename_dict)


def _process_df(df: pl.DataFrame, date: dt.date, contract_code: str) -> pl.DataFrame:
    """Process renamed legacy BMF DataFrame using Polars (parity with old Pandas)."""

    def _expiration_dates(raw: pl.Series) -> list[dt.date | None]:
        change_date = dt.date(2006, 5, 22)
        if date < change_date:
            return [_get_old_expiration_date(date, code) for code in raw.to_list()]
        day = 15 if contract_code == "DAP" else 1
        return [_get_expiration_date(code, day) for code in raw.to_list()]

    # Core columns
    exp_dates = _expiration_dates(df["ExpirationCode"])
    days_to_exp = [(d - date).days if d else None for d in exp_dates]
    bdays_to_exp = bday.count(date, exp_dates)

    df = df.with_columns(
        pl.Series("ExpirationDate", exp_dates).cast(pl.Date),
        pl.Series("DaysToExp", days_to_exp).cast(pl.Int64),
        pl.Series("BDaysToExp", bdays_to_exp),
        TradeDate=date,
        TickerSymbol=contract_code + pl.col("ExpirationCode"),
    ).filter(pl.col("DaysToExp") > 0)

    # Zero -> null conversion
    rate_cols = [c for c in df.columns if "Rate" in c]
    extra_cols = ["SettlementPrice"] if "SettlementPrice" in df.columns else []
    adj_cols = rate_cols + extra_cols
    df = df.with_columns(
        pl.when(pl.col(adj_cols) == 0)
        .then(pl.lit(None))
        .otherwise(pl.col(adj_cols))
        .name.keep()
    )

    # Rate transformation
    switch_date = dt.date(2002, 1, 17)
    if date <= switch_date and contract_code == "DI1":
        df = _adjust_older_contracts_rates(df, rate_cols)
    else:
        df = df.with_columns((pl.col(rate_cols) / 100).round(5))

    # SettlementRate
    count_conv = COUNT_CONVENTIONS.get(contract_code)
    if (
        count_conv in {BDAYS_PER_YEAR, CDAYS_PER_YEAR}
        and "SettlementPrice" in df.columns
    ):
        du_series = (
            df["BDaysToExp"] if count_conv == BDAYS_PER_YEAR else df["DaysToExp"]
        )
        settlement_rates = _convert_prices_to_rates(
            df["SettlementPrice"], du_series, count_conv
        )
        df = df.with_columns(SettlementRate=settlement_rates)

    # DV01
    if (
        contract_code == "DI1"
        and "SettlementRate" in df.columns
        and "SettlementPrice" in df.columns
    ):
        duration = pl.col("BDaysToExp") / BDAYS_PER_YEAR
        m_duration = duration / (1 + pl.col("SettlementRate"))
        df = df.with_columns(DV01=(0.0001 * m_duration * pl.col("SettlementPrice")))

    # Forward rates
    if contract_code in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        df = df.with_columns(
            ForwardRate=forwards(bdays=df["BDaysToExp"], rates=df["SettlementRate"])
        )

    return df


def _select_and_reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
    all_columns = [
        "TradeDate",
        "TickerSymbol",
        # "ExpirationCode",  # intentionally omitted
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        "OpenContracts",
        # "OpenContractsEndSession" removed for consistency with XML pipeline
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "DV01",
        "SettlementPrice",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "CloseAskRate",
        "CloseBidRate",
        "CloseRate",
        "SettlementRate",
        "ForwardRate",
    ]
    selected = [c for c in all_columns if c in df.columns]
    return df.select(selected)


def fetch_old_historical_df(date: dt.date, contract_code: str) -> pl.DataFrame:
    """
    Fetchs the futures data for a given date from B3.

    This function fetches and processes the futures data from B3 for a specific
    trade date. It's the primary external interface for accessing futures data.

    Args:
        date (dt.date): The trade date to fetch the futures data.
        contract_code (str): The asset code to fetch the futures data.

    Returns:
        pl.DataFrame: Processed futures data. If no data is found,
            returns an empty DataFrame.
    """
    html_text = _fetch_html_data(date, contract_code)
    if not html_text:
        return pl.DataFrame()
    df = _parse_raw_df(html_text)
    df = _pre_process_df(df)
    df = _rename_columns(df)
    df = _process_df(df, date, contract_code)
    df = _select_and_reorder_columns(df)
    return df
