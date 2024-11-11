import io
import locale
from dataclasses import dataclass
from typing import Literal

import pandas as pd
import python_calamine as pc
import requests


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
    response = requests.get(url, timeout=10)
    excel_data = io.BytesIO(response.content)

    # Load the workbook and select the first sheet
    workbook = pc.load_workbook(excel_data)
    first_sheet = workbook.sheet_names[0]
    data = workbook.get_sheet_by_name(first_sheet).to_python(skip_empty_area=True)

    # Extract projection update date and time from the first row
    update_str = str(data[0][0])
    last_update_str = update_str.split("Atualização:")[-1].strip()

    # Find the text containing the IPCA projection and extract its data
    ipca_data = next(line for line in data if "IPCA1" in line)
    ipca_text = str(ipca_data[-1])

    # Extract and format the reference month
    projection_text = str(ipca_data[1])
    month_str = projection_text.split("(")[-1].split(")")[0]
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
    ipca_month_ts = pd.to_datetime(month_str, format="%b/%y")
    reference_period = ipca_month_ts.to_period("M")
    locale.setlocale(locale.LC_TIME, "")  # Reset locale to default

    return IndicatorProjection(
        last_updated=pd.to_datetime(last_update_str, format="%d/%m/%Y - %H:%M h"),
        reference_period=reference_period,
        projected_value=round(float(ipca_text) / 100, 4),
    )
