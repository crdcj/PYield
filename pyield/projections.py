import io
import locale

import pandas as pd
import python_calamine as pc
import requests


def fetch_current_month_ipca_projection() -> dict:
    url = "https://www.anbima.com.br/informacoes/indicadores/arqs/indicadores.xls"
    response = requests.get(url)

    # from python_calamine import CalamineWorkbook
    excel_data = io.BytesIO(response.content)
    # workbook = CalamineWorkbook.load
    # workbook.sheets
    workbook = pc.load_workbook(excel_data)
    first_sheet = workbook.sheet_names[0]
    data = workbook.get_sheet_by_name(first_sheet).to_python(skip_empty_area=True)

    # File update date is in the first row
    # Row format: ['Data e Hora da Última Atualização: 19/04/2024 - 18:55 h', '', '']
    last_update_str = data[0][0].split("Atualização:")[-1].strip()
    last_update_pd = pd.to_datetime(last_update_str, format="%d/%m/%Y - %H:%M h")

    # Get row with the IPCA projection data
    for line in data:
        if "IPCA1" in line:
            print(line)
            ipca_data = line

    # IPCA row format: ['IPCA1', 'Projeção (abr/24)', 0.35]
    ipca_value = float(ipca_data[-1])
    month_data = ipca_data[1]

    # Extract text inside parenthesis: 'Projeção (abr/24)'
    month_begin = ipca_data[1].find("(")
    month_end = ipca_data[1].find(")")
    month_text = month_data[month_begin + 1 : month_end].strip()
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
    ipca_proj_month = pd.to_datetime(month_text, format="%b/%y")

    # Output dictionary
    ipca_dict = {}
    ipca_dict["updated_in"] = last_update_pd
    ipca_dict["month_of_reference"] = ipca_proj_month
    ipca_dict["value"] = ipca_value

    return ipca_dict
