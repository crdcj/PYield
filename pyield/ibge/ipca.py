import logging

import pandas as pd
import requests

from pyield import date_converter as dc
from pyield.config import default_retry

logger = logging.getLogger(__name__)


@default_retry
def ipca_monthly_rate(date: pd.Timestamp) -> float:
    """
    The function makes an API call to the IBGE's data portal to retrieve the data.
    An example of the API call: https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202403/variaveis/63?localidades=N1[all]
    where '202403' is the reference date in 'YYYYMM' format.
    The API URL is constructed dynamically based on the reference date provided.

    Examples:
        >>> yd.indicator("IPCA_MR", "01-04-2024")
        0.0038
    """
    date = dc.convert_input_dates(date)
    # Format the date as 'YYYYMM' for the API endpoint
    ipca_date = date.strftime("%Y%m")

    # Construct the API URL using the formatted date
    api_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/{ipca_date}/variaveis/63?localidades=N1[all]"

    response = requests.get(api_url)
    response.raise_for_status()  # Levanta uma exceção para códigos de erro HTTP
    data = response.json()
    if not data:
        msg = f"No data available for IPCA Monthly Rate on {date}"
        logger.warning(msg)
        return float("nan")
    ipca_str = data[0]["resultados"][0]["series"][0]["serie"][ipca_date]
    return round(float(ipca_str) / 100, 4)
