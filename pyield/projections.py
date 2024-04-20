import io
import locale

import pandas as pd
import python_calamine as pc
import requests


def fetch_current_month_ipca_projection() -> pd.Series:
    """
    Fetches the current month's IPCA projection from the ANBIMA website and returns it
    as a Pandas Series.

    This function retrieves and parses the excel file that contains economic indicators,
    specifically looking for the IPCA projection. It extracts the date of the last
    update and the IPCA projection for the reference month.

    Data file format example after parsing:
        ['Data e Hora da Última Atualização: 19/04/2024 - 18:55 h', '', '']
        ...
        ['IPCA1', 'Projeção (abr/24)', 0.35]
        ...

    Returns:
        pd.Series: A series containing:
              - 'updated_in': The datetime when the data was last updated,
              - 'reference_month': The month to which the IPCA projection applies,
              - 'value': The projected IPCA value as a float.

    Example of expected output:
        updated_in        2024-04-19 18:55:00
        reference_month   2024-04-01
        value             0.35
        dtype: object
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
    last_update_pd = pd.to_datetime(last_update_str, format="%d/%m/%Y - %H:%M h")

    # Find the row containing the IPCA projection and extract its data
    ipca_data = next(line for line in data if "IPCA1" in line)

    # Convert the last element of the IPCA data row to float for the projection value
    ipca_value = float(ipca_data[-1])

    # File update date is in the first row
    # Row format: ['Data e Hora da Última Atualização: 19/04/2024 - 18:55 h', '', '']
    last_update_str = data[0][0].split("Atualização:")[-1].strip()
    last_update_pd = pd.to_datetime(last_update_str, format="%d/%m/%Y - %H:%M h")

    # Extract the reference month from the text in parenthesis
    month_data = ipca_data[1]
    month_begin = ipca_data[1].find("(")
    month_end = ipca_data[1].find(")")
    month_text = month_data[month_begin + 1 : month_end].strip()
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
    ipca_proj_month = pd.to_datetime(month_text, format="%b/%y")
    # Reset locale to default
    locale.setlocale(locale.LC_TIME, "")
    formatted_month = ipca_proj_month.strftime("%b/%Y")

    # Create and return a Pandas Series
    result_series = pd.Series(
        {
            "updated_in": last_update_pd,
            "reference_month_date": ipca_proj_month,
            "reference_month_text": formatted_month,
            "value": ipca_value,
        }
    )

    return result_series
