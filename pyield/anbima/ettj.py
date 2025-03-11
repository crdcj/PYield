import logging
from io import StringIO

import pandas as pd
import requests

from pyield.forwards import forward_rates
from pyield.retry import default_retry

logger = logging.getLogger(__name__)
LAST_ETTJ_URL = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"

# Anbima ETTJ data has 4 decimal places in percentage values
# We will round to 6 decimal places to avoid floating point errors
ROUND_DIGITS = 6


@default_retry
def _get_last_content() -> str:
    """Fetches the raw yield curve data from ANBIMA."""
    request_payload = {
        "Idioma": "PT",
        "Dt_Ref": "",
        "saida": "csv",
    }
    r = requests.post(LAST_ETTJ_URL, data=request_payload)
    r.raise_for_status()
    return r.text


def _convert_text_to_df(text: str) -> pd.DataFrame:
    """Converts the raw yield curve text to a DataFrame."""
    raw_df = pd.read_csv(
        StringIO(text),
        sep=";",
        encoding="latin1",
        skiprows=5,
        dtype_backend="numpy_nullable",
    )

    idx = raw_df.query("Vertices =='PREFIXADOS (CIRCULAR 3.361)'").first_valid_index()  # noqa
    raw_df.query("index < @idx", inplace=True)
    csv_buffer = StringIO()

    # Save the DataFrame as CSV in memory buffer to be read again
    # It's easier than handling values with commas and dots directly in the DataFrame
    raw_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Move the buffer cursor to the beginning

    df = pd.read_csv(
        csv_buffer,
        decimal=",",
        thousands=".",
        dtype_backend="numpy_nullable",
    )
    file_date_str = text[0:10]  # 09/09/2024
    file_date = pd.to_datetime(file_date_str, format="%d/%m/%Y")
    df["date"] = file_date

    return df


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the raw yield curve DataFrame to calculate rates and forward rates."""
    # Rename columns
    rename_dict = {
        "Vertices": "vertex",
        "ETTJ IPCA": "real_rate",
        "ETTJ PREF": "nominal_rate",
        "Inflação Implícita": "implied_inflation",
    }
    df.rename(columns=rename_dict, inplace=True)

    # Divide float columns by 100 and round to 6 decimal places
    df["real_rate"] = (df["real_rate"] / 100).round(ROUND_DIGITS)
    df["nominal_rate"] = (df["nominal_rate"] / 100).round(ROUND_DIGITS)
    df["implied_inflation"] = (df["implied_inflation"] / 100).round(ROUND_DIGITS)

    df["real_rate_fwd"] = forward_rates(
        business_days=df["vertex"],
        zero_rates=df["real_rate"],
        groupby_dates=df["date"],
    )
    df["nominal_rate_fwd"] = forward_rates(
        business_days=df["vertex"],
        zero_rates=df["nominal_rate"],
        groupby_dates=df["date"],
    )
    df["implied_inflation_fwd"] = forward_rates(
        business_days=df["vertex"],
        zero_rates=df["implied_inflation"],
        groupby_dates=df["date"],
    )

    column_order = [
        "date",
        "vertex",
        "nominal_rate",
        "real_rate",
        "implied_inflation",
        "nominal_rate_fwd",
        "real_rate_fwd",
        "implied_inflation_fwd",
    ]
    df = df[column_order].copy()

    return df


def last_ettj() -> pd.DataFrame:
    """
    Retrieves and processes the latest Brazilian yield curve data from ANBIMA.

    This function fetches the most recent yield curve data published by ANBIMA,
    containing real rates (IPCA-indexed), nominal rates, and implied inflation
    at various vertices (time points). Both spot rates and forward rates are
    calculated and included in the returned DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the latest ETTJ data.

    DataFrame columns:
        - date: Reference date of the yield curve
        - vertex: Time point in business days
        - nominal_rate: Zero-coupon nominal interest rate
        - real_rate: Zero-coupon real interest rate (IPCA-indexed)
        - implied_inflation: Implied inflation rate (break-even inflation)
        - nominal_rate_fwd: Forward nominal interest rate
        - real_rate_fwd: Forward real interest rate
        - implied_inflation_fwd: Forward implied inflation rate

    Note:
        All rates are expressed in decimal format (e.g., 0.12 for 12%).
    """
    text = _get_last_content()
    df = _convert_text_to_df(text)
    return _process_df(df)
