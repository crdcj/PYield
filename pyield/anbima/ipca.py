import io
import locale
import warnings
from dataclasses import dataclass

import pandas as pd
import requests
from bs4 import XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


@dataclass
class IndicatorProjection:
    reference_period: pd.Period  # Reference month as a pd.Period object
    projected_value: float  # Projected value
    last_updated: pd.Timestamp  # Date and time of the last update


def ipca_projection() -> IndicatorProjection:
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
            - reference_period (pd.Period): Reference period of the projection
            - projected_value (float): Projected IPCA value as a decimal number
            - last_updated (pd.Timestamp): Date and time of the last data update

    Raises:
        requests.RequestException: If there are connection issues with the ANBIMA site
        ValueError: If the expected data is not found in the page structure
        locale.Error: If the 'pt_BR.UTF-8' locale is not available on the system

    Notes:
        - The function requires internet connection to access the ANBIMA website
        - The structure of the ANBIMA page may change, which could affect the function
        - Temporarily changes the locale to pt_BR.UTF-8 to process dates in Portuguese
    """
    url = "https://www.anbima.com.br/informacoes/indicadores/"
    r = requests.get(url)
    r.encoding = "latin1"
    dfs = pd.read_html(
        io.StringIO(r.text),
        flavor="html5lib",
        decimal=",",
        thousands=".",
        dtype_backend="numpy_nullable",
    )
    # The IPCA projection is in the third table
    df = dfs[2]

    last_update_str = df.iat[0, 0].split("Atualização:")[-1].strip()
    last_update = pd.to_datetime(last_update_str, format="%d/%m/%Y - %H:%M h")

    ipca_row = df.loc[df[0] == "IPCA1"]
    ipca_value = ipca_row.iloc[0, 2]
    ipca_value = pd.to_numeric(ipca_value, errors="coerce")
    ipca_value = round(float(ipca_value) / 100, 4)

    # Extract and format the reference month
    ipca_date = ipca_row.iloc[0, 1]
    ipca_date = str(ipca_date)
    ipca_date = ipca_date.split("(")[-1].split(")")[0]
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
    ipca_date = pd.to_datetime(ipca_date, format="%b/%y")
    reference_period = ipca_date.to_period("M")
    locale.setlocale(locale.LC_TIME, "")  # Reset locale to default

    return IndicatorProjection(
        last_updated=last_update,
        reference_period=reference_period,
        projected_value=ipca_value,
    )
