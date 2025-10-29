import datetime as dt
import io
from dataclasses import dataclass

import pandas as pd
import requests


@dataclass
class IndicatorProjection:
    last_updated: dt.datetime  # Date and time of the last update
    reference_period: str  # Reference month as a string in "MMM/YY" format
    projected_value: float  # Projected value


def _get_page_text() -> bytes:
    """Faz a requisição HTTP para a página da ANBIMA e retorna o texto HTML."""
    url = "https://www.anbima.com.br/informacoes/indicadores/"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.content
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Erro ao acessar a página da ANBIMA: {e}")


def _read_ipca_table(page_bytes: bytes) -> pd.DataFrame:
    dfs = pd.read_html(
        io.BytesIO(page_bytes),
        flavor="lxml",
        decimal=",",
        thousands=".",
        dtype_backend="pyarrow",
        encoding="latin1",
    )
    # The IPCA projection is in the third table
    df = dfs[2]
    return df


def projected_rate() -> IndicatorProjection:
    """
    Retrieves the current IPCA projection from the ANBIMA website.

    This function makes an HTTP request to the ANBIMA website, extracts HTML tables
    containing economic indicators, and specifically processes the IPCA projection data.

    Process:
        1. Accesses the ANBIMA indicators webpage
        2. Extracts the third table that contains the IPCA projection
        3. Locates the row labeled as "IPCA1"
        4. Extracts the projection value and converts it to decimal format
        5. Extracts and formats the reference month of the projection
        6. Extracts the date and time of the last update

    Returns:
        IndicatorProjection: An object containing:
            - last_updated (dt.datetime): Date and time of the last data update
            - reference_period (str): Reference period of the projection as a string in
              "MMM/YY" brazilian format (e.g., "set/25")
            - projected_value (float): Projected IPCA value as a decimal number

    Raises:
        requests.RequestException: If there are connection issues with the ANBIMA site
        ValueError: If the expected data is not found in the page structure

    Example:
        >>> from pyield import ipca
        >>> # Retrieve the current IPCA projection from ANBIMA
        >>> ipca.projected_rate()
        IndicatorProjection(last_updated=..., reference_period=..., projected_value=...)

    Notes:
        - The function requires internet connection to access the ANBIMA website
        - The structure of the ANBIMA page may change, which could affect the function
    """
    page_text = _get_page_text()
    df = _read_ipca_table(page_text)

    last_update_str = df.iat[0, 0].split("Atualização:")[-1].strip()
    last_update = dt.datetime.strptime(last_update_str, "%d/%m/%Y - %H:%M h")

    ipca_row = df.loc[df[0] == "IPCA1"]
    ipca_value = ipca_row.iloc[0, 2]
    ipca_value = float(ipca_value) / 100
    ipca_value = round(ipca_value, 4)

    # Extract and format the reference month
    ipca_date = ipca_row.iloc[0, 1]
    ipca_date = str(ipca_date)
    ipca_date = ipca_date.split("(")[-1].split(")")[0]

    return IndicatorProjection(
        last_updated=last_update,
        reference_period=ipca_date,
        projected_value=ipca_value,
    )
