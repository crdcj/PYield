import logging

import pandas as pd
import requests

from pyield.date_converter import DateScalar, convert_input_dates
from pyield.retry import default_retry

logger = logging.getLogger(__name__)
IPCA_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/"


@default_retry
def _fetch_series_data(url: str) -> dict[str, str]:
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Raises an exception for HTTP error codes
    data = response.json()
    if not data:
        raise ValueError(f"No data available for the API URL: {url}")
    return data[0]["resultados"][0]["series"][0]["serie"]


def _process_ipca_dataframe(
    data_dict: dict[str, str], is_in_pct: bool = False
) -> pd.DataFrame:
    """
    Process the IPCA data dictionary into a DataFrame with proper formatting.

    Args:
        data_dict (dict[str, str]): Dictionary containing the raw IPCA data
        is_in_pct (bool, optional): Whether the data represents rates in percentage
            format (True) or indexes (False). Defaults to False.

    Returns:
        pd.DataFrame: DataFrame with columns 'Period' and 'Value'
    """
    df = pd.DataFrame.from_dict(data_dict, orient="index")
    df = df.reset_index()
    df = df.rename(columns={"index": "Period", 0: "Value"})

    df["Value"] = pd.to_numeric(df["Value"])

    # If it's a rate value, divide by 100 and round to 4 decimal places
    if is_in_pct:
        df["Value"] = (df["Value"] / 100).round(4)
    else:
        df["Value"] = df["Value"].round(2)

    df["Value"] = df["Value"].astype("Float64")
    df["Period"] = pd.to_datetime(df["Period"], format="%Y%m").dt.to_period("M")

    return df


def ipca_rates(start: DateScalar, end: DateScalar) -> pd.DataFrame:
    """
    Retrieves the IPCA monthly rates for a specified date range.

    Makes an API call to the IBGE's data portal using the format:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/63?localidades=N1[all]

    Example: For the date range "01-01-2024" to "31-03-2024", the API URL will be:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202401-202403/variaveis/63?localidades=N1[all]

    Args:
        start (DateScalar): The start date of the date range
        end (DateScalar): The end date of the date range

    Returns:
        pd.DataFrame: DataFrame with columns 'Period' and 'Rate'

    Examples:
        >>> from pyield import ibge
        >>> # Get the IPCA rates for the first quarter of 2024
        >>> df = ibge.ipca_rates("01-01-2024", "31-03-2024")
    """
    start = convert_input_dates(start)
    end = convert_input_dates(end)

    start_date = start.strftime("%Y%m")
    end_date = end.strftime("%Y%m")
    api_url = f"{IPCA_URL}{start_date}-{end_date}/variaveis/63?localidades=N1[all]"
    data_dict = _fetch_series_data(api_url)

    return _process_ipca_dataframe(data_dict, is_in_pct=True)


def ipca_last_rates(num_months: int = 1) -> pd.DataFrame:
    """
    Retrieves the last IPCA monthly rates for a specified number of months.

    Makes an API call to the IBGE's data portal using the format:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/63?localidades=N1[all]

    Example: For the last 2 months, the API URL will be:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-2/variaveis/63?localidades=N1[all]

    Args:
        num_months (int, optional): Number of months to retrieve. Defaults to 1.

    Returns:
        pd.DataFrame: DataFrame with columns 'Period' and 'Value'

    Raises:
        ValueError: If num_months is 0

    Examples:
        >>> from pyield import ibge
        >>> # Get the last month's IPCA rate
        >>> df = ibge.ipca_last_rates(1)
        >>> # Get the last 3 months' IPCA rates
        >>> df = ibge.ipca_last_rates(3)
    """
    num_months = abs(num_months)
    if num_months == 0:
        raise ValueError("The number of months must be greater than 0.")

    api_url = f"{IPCA_URL}-{num_months}/variaveis/63?localidades=N1[all]"
    data_dict = _fetch_series_data(api_url)

    return _process_ipca_dataframe(data_dict, is_in_pct=True)


def ipca_last_indexes(num_months: int = 1) -> pd.DataFrame:
    """
    Retrieves the last IPCA index values for a specified number of months.

    Makes an API call to the IBGE's data portal using the format:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/2266?localidades=N1[all]

    Example: For the last 2 months, the API URL will be:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-2/variaveis/2266?localidades=N1[all]

    Args:
        num_months (int, optional): Number of months to retrieve. Defaults to 1.

    Returns:
        pd.DataFrame: DataFrame with columns 'Period' and 'Value'

    Raises:
        ValueError: If num_months is 0

    Examples:
        >>> from pyield import ibge
        >>> # Get the last month's IPCA index
        >>> df = ibge.ipca_last_indexes(1)
        >>> # Get the last 3 months' IPCA indexes
        >>> df = ibge.ipca_last_indexes(3)
    """
    num_months = abs(num_months)
    if num_months == 0:
        raise ValueError("The number of months must be greater than 0.")

    api_url = f"{IPCA_URL}-{num_months}/variaveis/2266?localidades=N1[all]"
    data_dict = _fetch_series_data(api_url)

    return _process_ipca_dataframe(data_dict)


def ipca_indexes(start: DateScalar, end: DateScalar) -> pd.DataFrame:
    """
    Retrieves the IPCA index values for a specified date range.

    Makes an API call to the IBGE's data portal using the format:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/2266?localidades=N1[all]

    Example: For the date range "01-01-2024" to "31-03-2024", the API URL will be:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202401-202403/variaveis/2266?localidades=N1[all]

    Args:
        start (DateScalar): The start date of the date range
        end (DateScalar): The end date of the date range

    Returns:
        pd.DataFrame: DataFrame with columns 'Period' and 'Value'

    Examples:
        >>> from pyield import ibge
        >>> # Get the IPCA indexes for the first quarter of 2024
        >>> df = ibge.ipca_indexes("01-01-2024", "31-03-2024")
    """
    start = convert_input_dates(start)
    end = convert_input_dates(end)

    start_date = start.strftime("%Y%m")
    end_date = end.strftime("%Y%m")
    api_url = f"{IPCA_URL}{start_date}-{end_date}/variaveis/2266?localidades=N1[all]"
    data_dict = _fetch_series_data(api_url)

    return _process_ipca_dataframe(data_dict)
