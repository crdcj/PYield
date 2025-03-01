import locale
from dataclasses import dataclass

import pandas as pd


@dataclass
class IndicatorProjection:
    reference_period: pd.Period  # Reference month as a pd.Period object
    projected_value: float  # Projected value
    last_updated: pd.Timestamp  # Date and time of the last update


def ipca_projection() -> IndicatorProjection:
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
    df = pd.read_excel(url, dtype_backend="numpy_nullable")

    last_update_str = df.columns[0].split("Atualização:")[-1].strip()
    last_update = pd.to_datetime(last_update_str, format="%d/%m/%Y - %H:%M h")

    ipca_row = df[df.iloc[:, 0] == "IPCA1"]

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
