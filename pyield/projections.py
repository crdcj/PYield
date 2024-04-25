import io
import locale
from dataclasses import dataclass

import pandas as pd
import python_calamine as pc
import requests


@dataclass
class IndicatorProjection:
    last_updated: pd.Timestamp  # Date and time of the last update
    reference_month_ts: pd.Timestamp  # Timestamp to which the projection applies
    reference_month_br: str  # Brazilian formatted month (e.g., "ABR/2024")
    projected_value: float  # Projected value


def fetch_current_month_ipca_projection() -> IndicatorProjection:
    """
    Fetches the current month's IPCA projection from the ANBIMA website and returns it
    as an IndicatorProjection instance.

    This function retrieves and parses the Excel file that contains economic indicators,
    specifically looking for the IPCA projection. It extracts the date of the last
    update and the IPCA projection for the reference month.

    Args:
        None

    Returns:
        IndicatorProjection: An instance of IndicatorProjection containing:
            - last_updated (pd.Timestamp): The datetime when the data was last updated.
            - reference_month_ts (pd.Timestamp): The month to which the IPCA projection
              applies.
            - reference_month_br (str): The formatted month as a string
              (e.g., "ABR/2024") using the pt_BR locale.
            - projected_value (float): The projected IPCA value.

    Example:
        >>> projection = fetch_current_month_ipca_projection()
        >>> print(projection)
        IndicatorProjection(
            last_updated=pd.Timestamp('2024-04-19 18:55:00'),
            reference_month_ts=pd.Timestamp('2024-04-01 00:00:00'),
            reference_month_br='ABR/2024',
            projected_value=0.35
        )

    Data file format example after parsing:
        - ['Data e Hora da Última Atualização: 19/04/2024 - 18:55 h', '', '']
        - ...
        - ['IPCA1', 'Projeção (abr/24)', 0.35]
        - ...
    """
    # Define the URL and get the data
    url = "https://www.anbima.com.br/informacoes/indicadores/arqs/indicadores.xls"
    response = requests.get(url)
    excel_data = io.BytesIO(response.content)

    # Load the workbook and select the first sheet
    workbook = pc.load_workbook(excel_data)
    first_sheet = workbook.sheet_names[0]
    data = workbook.get_sheet_by_name(first_sheet).to_python(skip_empty_area=True)

    # Extract projection update date and time from the first row
    update_str = str(data[0][0])
    last_update_str = update_str.split("Atualização:")[-1].strip()
    last_updated = pd.to_datetime(last_update_str, format="%d/%m/%Y - %H:%M h")

    # Find the row containing the IPCA projection and extract its data
    ipca_data = next(line for line in data if "IPCA1" in line)
    ipca_text = str(ipca_data[-1])

    # Convert the last element of the IPCA data row to float for the projection value
    ipca_value = round(float(ipca_text) / 100, 4)

    # Extract and format the reference month
    projection_text = str(ipca_data[1])
    month_str = projection_text.split("(")[-1].split(")")[0]
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
    ipca_month_ts = pd.to_datetime(month_str, format="%b/%y")
    ipca_month_br = ipca_month_ts.strftime("%b/%Y").upper()
    locale.setlocale(locale.LC_TIME, "")  # Reset locale to default

    return IndicatorProjection(
        last_updated=last_updated,
        reference_month_ts=ipca_month_ts,
        reference_month_br=ipca_month_br,
        projected_value=ipca_value,
    )
