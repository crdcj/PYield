import locale
from dataclasses import dataclass
from typing import Literal

import pandas as pd


@dataclass
class IndicatorProjection:
    reference_period: pd.Period  # Reference month as a pd.Period object
    projected_value: float  # Projected value
    last_updated: pd.Timestamp  # Date and time of the last update


def projection(projection_code: Literal["IPCA_CM"]) -> IndicatorProjection:
    """
    Fetches the projected value of an economic indicator for the current month.

    This function retrieves the projected value of an economic indicator for the current
    month. The correct data source is dynamically chosen based on the projection code
    provided.

    Args:
        projection_code (Literal["IPCA_CM"]): The code for the desired projection:
            - "IPCA_CM": IPCA (monthly inflation) projection for the current month.

    Returns:
        IndicatorProjection: An instance of IndicatorProjection containing:
            - last_updated (pd.Timestamp): The datetime when the data was last updated.
            - reference_month_ts (pd.Timestamp): The month to which the IPCA projection
              applies.
            - reference_month_br (str): The formatted month as a string
              (e.g., "ABR/2024") using the pt_BR locale.
            - projected_value (float): The projected IPCA value.

    Examples:
        >>> projection("IPCA_CM")
        IndicatorProjection(reference_period=Period(...), projected_value=..., ...)

    """
    proj_type = str(projection_code).upper()
    if proj_type == "IPCA_CM":
        return ipca_current_month()
    else:
        raise ValueError(f"Invalid projection type: {proj_type}")


def ipca_current_month() -> IndicatorProjection:
    """
    This function retrieves and parses the Excel file that contains economic indicators,
    specifically looking for the IPCA projection. It extracts the date of the last
    update and the IPCA projection for the reference month.

    Data file format example after parsing:
        - ['Data e Hora da Última Atualização: 19/04/2024 - 18:55 h', '', '']
        - ...
        - ['IPCA1', 'Projeção (abr/24)', 0.35]
        - ...
    """
    # Define the URL and get the data
    url = "https://www.anbima.com.br/informacoes/indicadores/arqs/indicadores.xls"
    df = pd.read_excel(url)

    last_update_str = df.columns[0].split("Atualização:")[-1].strip()

    ipca_row = df[df.iloc[:, 0] == "IPCA1"]
    ipca_date = ipca_row.iloc[0, 1]
    ipca_value = ipca_row.iloc[0, 2]

    # Extract and format the reference month
    ipca_date = ipca_date.split("(")[-1].split(")")[0]
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
    ipca_date = pd.to_datetime(ipca_date, format="%b/%y")
    reference_period = ipca_date.to_period("M")
    locale.setlocale(locale.LC_TIME, "")  # Reset locale to default

    return IndicatorProjection(
        last_updated=pd.to_datetime(last_update_str, format="%d/%m/%Y - %H:%M h"),
        reference_period=reference_period,
        projected_value=round(float(ipca_value) / 100, 4),
    )
