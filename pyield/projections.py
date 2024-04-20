import io
import locale

import pandas as pd
import python_calamine as pc
import requests


def fetch_current_month_ipca_projection() -> dict:
    """
    Fetches the current month's IPCA projection from the ANBIMA website and returns it
    as a dictionary.

    This function retrieves and parses the Excel file that contains economic indicators,
    specifically looking for the IPCA projection. It extracts the date of the last
    update and the IPCA projection for the reference month.

    Args:
        None

    Returns:
        dict: A dictionary containing:
            'update_datetime' (datetime.datetime): The datetime when the data was last
                updated.
            'projection_month_ts' (datetime.datetime): The month to which the
                IPCA projection applies as a Timestamp.
            'projection_month_str' (str): The formatted month as a string
                (e.g., "ABR/2024") using the pt_BR locale.
            'ipca_value' (float): The projected IPCA value.

    Examples:
        >>> fetch_current_month_ipca_projection()
        {
            'update_datetime': pd.Timestamp('2024-04-19 18:55:00'),
            'projection_month_ts': pd.Timestamp('2024-04-01'),
            'projection_month_str': "ABR/2024",
            'ipca_value': 0.35
        }

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

    # Extract projection update date from the first row
    last_update_str = data[0][0].split("Atualização:")[-1].strip()
    last_update_ts = pd.to_datetime(last_update_str, format="%d/%m/%Y - %H:%M h")

    # Find the row containing the IPCA projection and extract its data
    ipca_data = next(line for line in data if "IPCA1" in line)

    # Convert the last element of the IPCA data row to float for the projection value
    ipca_value = float(ipca_data[-1])

    # Extract and format the reference month
    month_data = ipca_data[1]
    month_begin = month_data.find("(")
    month_end = month_data.find(")")
    month_text = month_data[month_begin + 1 : month_end].strip()
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
    projection_month_ts = pd.to_datetime(month_text, format="%b/%y")
    # locale.setlocale(locale.LC_TIME, "")  # Reset locale to default
    projection_month_str = projection_month_ts.strftime("%b/%Y").upper()

    # Create and return a dictionary with the results
    result_dict = {
        "last_update_ts": last_update_ts,
        "projection_month_ts": projection_month_ts,
        "projection_month_str": projection_month_str,
        "ipca_value": ipca_value,
    }

    return result_dict
