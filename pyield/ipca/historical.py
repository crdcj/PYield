import logging

import polars as pl
import requests

from pyield.converters import convert_dates
from pyield.retry import default_retry
from pyield.types import DateLike, has_nullable_args

logger = logging.getLogger(__name__)
IPCA_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/"


@default_retry
def _fetch_api_data(url: str) -> dict[str, str]:
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Raises an exception for HTTP error codes
    data = response.json()
    if not data:
        raise ValueError(f"No data available for the API URL: {url}")
    return data[0]["resultados"][0]["series"][0]["serie"]


def _process_ipca_dataframe(
    data_dict: dict[str, str], is_in_pct: bool = False
) -> pl.DataFrame:
    """
    Process the IPCA data dictionary into a DataFrame with proper formatting.

    Args:
        data_dict (dict[str, str]): Dictionary containing the raw IPCA data
        is_in_pct (bool, optional): Whether the data represents rates in percentage
            format (True) or indexes (False). Defaults to False.

    Returns:
        pl.DataFrame: DataFrame with columns 'Period' and 'Value'
    """
    df = pl.DataFrame(
        {"Period": data_dict.keys(), "Value": data_dict.values()}
    ).with_columns(
        pl.col("Period").cast(pl.Int64),
        pl.col("Value").cast(pl.Float64),
    )
    if is_in_pct:
        df = df.with_columns((pl.col("Value") / 100).round(4))
    return df


def rates(start: DateLike, end: DateLike) -> pl.DataFrame:
    """
    Retrieves the IPCA monthly rates for a specified date range.

    Makes an API call to the IBGE's data portal using the format:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/63?localidades=N1[all]

    Example: For the date range "01-01-2024" to "31-03-2024", the API URL will be:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202401-202403/variaveis/63?localidades=N1[all]

    Args:
        start (DateLike): The start date of the date range
        end (DateLike): The end date of the date range

    Returns:
        pl.DataFrame: DataFrame with columns 'Period' and 'Rate'

    Examples:
        >>> from pyield import ipca
        >>> # Get the IPCA rates for the first quarter of 2025
        >>> ipca.rates("01-01-2025", "01-03-2025")
        shape: (3, 2)
        ┌────────┬────────┐
        │ Period ┆ Value  │
        │ ---    ┆ ---    │
        │ i64    ┆ f64    │
        ╞════════╪════════╡
        │ 202501 ┆ 0.0016 │
        │ 202502 ┆ 0.0131 │
        │ 202503 ┆ 0.0056 │
        └────────┴────────┘
    """
    if has_nullable_args(start, end):
        return pl.DataFrame()
    start = convert_dates(start)
    end = convert_dates(end)

    start_date = start.strftime("%Y%m")
    end_date = end.strftime("%Y%m")
    api_url = f"{IPCA_URL}{start_date}-{end_date}/variaveis/63?localidades=N1[all]"
    data_dict = _fetch_api_data(api_url)

    return _process_ipca_dataframe(data_dict, is_in_pct=True)


def last_rates(num_months: int = 1) -> pl.DataFrame:
    """
    Retrieves the last IPCA monthly rates for a specified number of months.

    Makes an API call to the IBGE's data portal using the format:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/63?localidades=N1[all]

    Example: For the last 2 months, the API URL will be:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-2/variaveis/63?localidades=N1[all]

    Args:
        num_months (int, optional): Number of months to retrieve. Defaults to 1.

    Returns:
        pl.DataFrame: DataFrame with columns 'Period' and 'Value'

    Raises:
        ValueError: If num_months is 0

    Examples:
        >>> from pyield import ipca
        >>> # Get the last month's IPCA rate
        >>> df = ipca.last_rates(1)
        >>> # Get the last 3 months' IPCA rates
        >>> df = ipca.last_rates(3)
    """
    num_months = abs(num_months)
    if num_months == 0:
        raise ValueError("The number of months must be greater than 0.")

    api_url = f"{IPCA_URL}-{num_months}/variaveis/63?localidades=N1[all]"
    data_dict = _fetch_api_data(api_url)

    return _process_ipca_dataframe(data_dict, is_in_pct=True)


def last_indexes(num_months: int = 1) -> pl.DataFrame:
    """
    Retrieves the last IPCA index values for a specified number of months.

    Makes an API call to the IBGE's data portal using the format:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/2266?localidades=N1[all]

    Example: For the last 2 months, the API URL will be:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-2/variaveis/2266?localidades=N1[all]

    Args:
        num_months (int, optional): Number of months to retrieve. Defaults to 1.

    Returns:
        pl.DataFrame: DataFrame with columns 'Period' and 'Value'

    Examples:
        >>> from pyield import ipca
        >>> # Get the last month's IPCA index
        >>> df = ipca.last_indexes(1)
        >>> # Get the last 3 months' IPCA indexes
        >>> df = ipca.last_indexes(3)
    """
    num_months = abs(num_months)
    if num_months == 0:
        return pl.DataFrame()

    api_url = f"{IPCA_URL}-{num_months}/variaveis/2266?localidades=N1[all]"
    data_dict = _fetch_api_data(api_url)

    return _process_ipca_dataframe(data_dict)


def indexes(start: DateLike, end: DateLike) -> pl.DataFrame:
    """
    Retrieves the IPCA index values for a specified date range.

    Makes an API call to the IBGE's data portal using the format:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/2266?localidades=N1[all]

    Example: For the date range "01-01-2024" to "31-03-2024", the API URL will be:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202401-202403/variaveis/2266?localidades=N1[all]

    Args:
        start (DateLike): The start date of the date range
        end (DateLike): The end date of the date range

    Returns:
        pl.DataFrame: DataFrame with columns 'Period' and 'Value'

    Examples:
        >>> from pyield import ipca
        >>> # Get the IPCA indexes for the first quarter of 2025
        >>> ipca.indexes(start="01-01-2025", end="01-03-2025")
        shape: (3, 2)
        ┌────────┬─────────┐
        │ Period ┆ Value   │
        │ ---    ┆ ---     │
        │ i64    ┆ f64     │
        ╞════════╪═════════╡
        │ 202501 ┆ 7111.86 │
        │ 202502 ┆ 7205.03 │
        │ 202503 ┆ 7245.38 │
        └────────┴─────────┘
    """
    if has_nullable_args(start, end):
        return pl.DataFrame()
    start = convert_dates(start)
    end = convert_dates(end)

    start_date = start.strftime("%Y%m")
    end_date = end.strftime("%Y%m")
    api_url = f"{IPCA_URL}{start_date}-{end_date}/variaveis/2266?localidades=N1[all]"
    data_dict = _fetch_api_data(api_url)

    return _process_ipca_dataframe(data_dict)
