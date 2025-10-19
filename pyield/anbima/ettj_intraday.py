import datetime as dt
import logging
from io import StringIO

import polars as pl
import requests

logger = logging.getLogger(__name__)

INTRADAY_ETTJ_URL = (
    "https://www.anbima.com.br/informacoes/curvas-intradiarias/cIntra-down.asp"
)

# Anbima ETTJ data has 4 decimal places in percentage values
# We will round to 6 decimal places to avoid floating point errors
ROUND_DIGITS = 6


def _fetch_intraday_text() -> str:
    request_payload = {"Dt_Ref": "", "saida": "csv"}
    response = requests.post(INTRADAY_ETTJ_URL, data=request_payload)
    response.raise_for_status()
    return response.text


def _extrair_secao(
    linhas: list[str], indice_inicio: int, indice_fim: int | None = None
) -> tuple[str, str]:
    """
    Extrai a data de ref. e o conteúdo de uma tabela a partir de uma lista de linhas.

    Args:
        linhas: A lista completa de linhas do texto.
        indice_inicio: O índice da linha onde o título da seção começa.
        indice_fim: O índice da linha onde a seção termina (exclusivo).
            Se None, vai até o final.

    Returns:
        Uma tupla contendo (string_da_data, string_da_tabela).
    """
    # A data está sempre na linha seguinte ao título
    data_ref_str = linhas[indice_inicio + 1]

    # As linhas da tabela começam duas linhas após o título e vão até o fim da seção
    tabela_linhas = linhas[indice_inicio + 2 : indice_fim]
    tabela_str = "\n".join(tabela_linhas).replace(".", "").replace(",", ".")

    return data_ref_str, tabela_str


def _extract_date_and_tables(texto: str) -> tuple[dt.date, str, str]:
    """Função principal para extrair as tabelas de forma modular."""
    # Títulos que servem como nossos marcadores
    titulo_pre = "ETTJ PREFIXADOS (%a.a./252)"
    titulo_ipca = "ETTJ IPCA (%a.a./252)"

    # Pré-processamento do texto
    linhas = [linha for linha in texto.strip().splitlines() if linha.strip()]

    # Encontrar os índices dos marcadores
    try:
        inicio_tabela_pre = linhas.index(titulo_pre)
        inicio_tabela_ipca = linhas.index(titulo_ipca)
    except ValueError as e:
        raise ValueError(
            f"Não foi possível encontrar um dos títulos marcadores no texto: {e}"
        )

    # --- Extrair Tabela 1 (PREFIXADOS) usando a função auxiliar ---
    # A primeira tabela vai do seu início até o início da segunda.
    data_ref_pre, tabela_pre = _extrair_secao(
        linhas, inicio_tabela_pre, inicio_tabela_ipca
    )

    # --- Extrair Tabela 2 (IPCA) usando a mesma função auxiliar ---
    # A segunda tabela vai do seu início até o final do texto.
    data_ref_ipca, tabela_ipca = _extrair_secao(
        linhas,
        inicio_tabela_ipca,
        None,  # O 'None' faz o slice ir até o fim da lista
    )

    # Validação e conversão da data
    if data_ref_pre != data_ref_ipca:
        raise ValueError(
            f"Datas de ref. diferentes: PRE='{data_ref_pre}', IPCA='{data_ref_ipca}'"
        )
    data_ref = dt.datetime.strptime(data_ref_pre, "%d/%m/%Y").date()

    return data_ref, tabela_pre, tabela_ipca


def _parse_intraday_table(texto: str) -> pl.DataFrame:
    return pl.read_csv(StringIO(texto), separator=";").drop("Fechamento D -1")


def intraday_ettj() -> pl.DataFrame:
    """
    Retrieves and processes the intraday Brazilian yield curve data from ANBIMA.

    This function fetches the most recent intraday yield curve data published by ANBIMA,
    containing real rates (IPCA-indexed), nominal rates, and implied inflation
    at various vertices (time points). The curve is published at around 12:30 PM BRT.

    Returns:
        pl.DataFrame: A DataFrame containing the intraday ETTJ data.

    DataFrame columns:
        - date: Reference date of the yield curve
        - vertex: Time point in business days
        - nominal_rate: Zero-coupon nominal interest rate
        - real_rate: Zero-coupon real interest rate (IPCA-indexed)
        - implied_inflation: Implied inflation rate (break-even inflation)

    Note:
        All rates are expressed in decimal format (e.g., 0.12 for 12%).
    """
    api_text = _fetch_intraday_text()

    # --- Extração da Tabela 1: PREFIXADOS ---
    data_ref, tabela_pre, tabela_ipca = _extract_date_and_tables(api_text)

    df_pre = _parse_intraday_table(tabela_pre)
    df_pre = df_pre.rename({"D0": "nominal_rate"})

    df_ipca = _parse_intraday_table(tabela_ipca)
    df_ipca = df_ipca.rename({"D0": "real_rate"})

    df = df_pre.join(df_ipca, on="Vertices", how="right")
    df = df.rename({"Vertices": "vertex"})

    df = df.with_columns(
        # convertendo de % para decimal e arredondando
        (pl.col("real_rate") / 100).round(ROUND_DIGITS),
        (pl.col("nominal_rate") / 100).round(ROUND_DIGITS),
        pl.lit(data_ref).alias("date"),
    ).with_columns(
        ((pl.col("nominal_rate") + 1) / (pl.col("real_rate") + 1) - 1)
        .round(ROUND_DIGITS)
        .alias("implied_inflation"),
    )
    column_order = ["date", "vertex", "nominal_rate", "real_rate", "implied_inflation"]
    return df.select(column_order)
