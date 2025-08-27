import logging
from io import StringIO

import pandas as pd
import requests

from pyield.retry import default_retry

logger = logging.getLogger(__name__)
LAST_ETTJ_URL = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"
INTRADAY_ETTJ_URL = (
    "https://www.anbima.com.br/informacoes/curvas-intradiarias/cIntra-down.asp"
)

# Anbima ETTJ data has 4 decimal places in percentage values
# We will round to 6 decimal places to avoid floating point errors
ROUND_DIGITS = 6


@default_retry
def _get_last_content_text() -> str:
    """Fetches the raw yield curve data from ANBIMA."""
    request_payload = {
        "Idioma": "PT",
        "Dt_Ref": "",
        "saida": "csv",
    }
    r = requests.post(LAST_ETTJ_URL, data=request_payload)
    r.raise_for_status()
    return r.text


def _get_reference_date(text: str) -> pd.Timestamp:
    file_date_str = text[0:10]  # formato = 09/09/2024
    file_date = pd.to_datetime(file_date_str, format="%d/%m/%Y")
    return file_date


def _filter_ettf_text(texto_completo: str) -> str:
    # Definir os marcadores de início e fim
    marcador_inicio = "Vertices;ETTJ IPCA;ETTJ PREF;Inflação Implícita"
    marcador_fim = "PREFIXADOS (CIRCULAR 3.361)"

    # 2. Dividir o texto em uma lista de linhas
    linhas = texto_completo.strip().splitlines()

    # 3. Encontrar os índices das linhas de início e fim
    indice_inicio = linhas.index(marcador_inicio)
    indice_fim = linhas.index(marcador_fim)

    # 4. Fatiar a lista para extrair o trecho desejado
    trecho_filtrado = linhas[indice_inicio:indice_fim]

    # Remover linhas vazias que possam ter sido incluídas no final
    while trecho_filtrado and not trecho_filtrado[-1].strip():
        trecho_filtrado.pop()

    # 5. Juntar as linhas filtradas em um único texto e retornar
    return "\n".join(trecho_filtrado)


def _convert_text_to_df(text: str, reference_date: pd.Timestamp) -> pd.DataFrame:
    """Converts the raw yield curve text to a DataFrame."""
    df = pd.read_csv(
        StringIO(text),
        sep=";",
        decimal=",",
        thousands=".",
        encoding="latin1",
        dtype_backend="pyarrow",
    )
    df["date"] = reference_date
    df["date"] = df["date"].astype("date32[pyarrow]")

    return df


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the raw yield curve DataFrame to calculate rates and forward rates."""
    # Rename columns
    rename_dict = {
        "Vertices": "vertex",
        "ETTJ IPCA": "real_rate",
        "ETTJ PREF": "nominal_rate",
        "Inflação Implícita": "implied_inflation",
    }
    df.rename(columns=rename_dict, inplace=True)

    # Divide float columns by 100 and round to 6 decimal places
    df["real_rate"] = (df["real_rate"] / 100).round(ROUND_DIGITS)
    df["nominal_rate"] = (df["nominal_rate"] / 100).round(ROUND_DIGITS)
    df["implied_inflation"] = (df["implied_inflation"] / 100).round(ROUND_DIGITS)

    column_order = [
        "date",
        "vertex",
        "nominal_rate",
        "real_rate",
        "implied_inflation",
    ]
    return df[column_order].copy()


def last_ettj() -> pd.DataFrame:
    """
    Retrieves and processes the latest Brazilian yield curve data from ANBIMA.

    This function fetches the most recent yield curve data published by ANBIMA,
    containing real rates (IPCA-indexed), nominal rates, and implied inflation
    at various vertices (time points).

    Returns:
        pd.DataFrame: A DataFrame containing the latest ETTJ data.

    DataFrame columns:
        - date: Reference date of the yield curve
        - vertex: Time point in business days
        - nominal_rate: Zero-coupon nominal interest rate
        - real_rate: Zero-coupon real interest rate (IPCA-indexed)
        - implied_inflation: Implied inflation rate (break-even inflation)

    Note:
        All rates are expressed in decimal format (e.g., 0.12 for 12%).
    """
    text = _get_last_content_text()
    reference_date = _get_reference_date(text)
    text = _filter_ettf_text(text)
    df = _convert_text_to_df(text, reference_date)
    return _process_df(df)


def _extrair_data_e_tabelas(
    texto: str, titulo_tabela: str
) -> tuple[pd.Timestamp, str, str]:
    # Títulos que servem como nossos marcadores
    titulo_pre = "ETTJ PREFIXADOS (%a.a./252)"
    titulo_ipca = "ETTJ IPCA (%a.a./252)"

    # Dividir o texto em linhas
    linhas = [linha for linha in texto.strip().splitlines() if linha.strip()]

    # Encontrar os índices dos títulos
    inicio_tabela_pre = linhas.index(titulo_pre)
    inicio_tabela_ipca = linhas.index(titulo_ipca)

    # --- Extrair Tabela 1 ---
    data_ref_pre = linhas[inicio_tabela_pre + 1]
    # A tabela 1 vai do seu cabeçalho até a linha antes do título da tabela 2
    tabela_pre_linhas = linhas[inicio_tabela_pre + 2 : inicio_tabela_ipca]
    tabela_pre = "\n".join(tabela_pre_linhas)

    # --- Extrair Tabela 2 ---
    data_ref_ipca = linhas[inicio_tabela_ipca + 1]
    # A tabela 2 vai do seu cabeçalho até o final da lista
    tabela_ipca_linhas = linhas[inicio_tabela_ipca + 2 :]
    tabela_ipca = "\n".join(tabela_ipca_linhas)

    if data_ref_pre != data_ref_ipca:
        raise ValueError("Datas de referência diferentes")

    data_ref = pd.to_datetime(data_ref_pre, format="%d/%m/%Y")

    return data_ref, tabela_pre, tabela_ipca


def _ler_tabela_intradia(texto: str) -> pd.DataFrame:
    """
    Reads a table from the given text and returns it as a DataFrame.

    Args:
        texto (str): The text containing the table data.

    Returns:
        pd.DataFrame: A DataFrame containing the table data.
    """
    df = pd.read_csv(
        StringIO(texto),
        sep=";",
        decimal=",",
        thousands=".",
        dtype_backend="pyarrow",
    )
    df = df.drop(columns=["Fechamento D -1"])
    return df


def intraday_ettj() -> pd.DataFrame:
    """
    Retrieves and processes the intraday Brazilian yield curve data from ANBIMA.

    This function fetches the most recent intraday yield curve data published by ANBIMA,
    containing real rates (IPCA-indexed), nominal rates, and implied inflation
    at various vertices (time points). The curve is published at around 12:30 PM BRT.

    Returns:
        pd.DataFrame: A DataFrame containing the intraday ETTJ data.

    DataFrame columns:
        - date: Reference date of the yield curve
        - vertex: Time point in business days
        - nominal_rate: Zero-coupon nominal interest rate
        - real_rate: Zero-coupon real interest rate (IPCA-indexed)
        - implied_inflation: Implied inflation rate (break-even inflation)

    Note:
        All rates are expressed in decimal format (e.g., 0.12 for 12%).
    """

    request_payload = {"Dt_Ref": "", "saida": "csv"}
    response = requests.post(INTRADAY_ETTJ_URL, data=request_payload)
    texto = response.text

    # --- Extração da Tabela 1: PREFIXADOS ---
    data_ref, tabela_pre, tabela_ipca = _extrair_data_e_tabelas(texto, texto)

    df_pre = _ler_tabela_intradia(tabela_pre)
    df_pre = df_pre.rename(columns={"D0": "nominal_rate"})

    df_ipca = _ler_tabela_intradia(tabela_ipca)
    df_ipca = df_ipca.rename(columns={"D0": "real_rate"})

    df = pd.merge(df_pre, df_ipca, on="Vertices", how="right")
    df = df.rename(columns={"Vertices": "vertex"})

    # Divide float columns by 100 and round to 6 decimal places
    df["real_rate"] = (df["real_rate"] / 100).round(ROUND_DIGITS)
    df["nominal_rate"] = (df["nominal_rate"] / 100).round(ROUND_DIGITS)
    df["implied_inflation"] = (df["nominal_rate"] + 1) / (df["real_rate"] + 1) - 1
    df["implied_inflation"] = df["implied_inflation"].round(ROUND_DIGITS)

    df["date"] = data_ref
    df["date"] = df["date"].astype("date32[pyarrow]")
    column_order = ["date", "vertex", "nominal_rate", "real_rate", "implied_inflation"]
    return df[column_order].copy()
