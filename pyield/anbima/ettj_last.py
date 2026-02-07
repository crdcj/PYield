import datetime as dt
import logging
from io import StringIO

import polars as pl
import requests

from pyield.retry import default_retry

logger = logging.getLogger(__name__)
LAST_ETTJ_URL = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"

# Anbima ETTJ data has 4 decimal places in percentage values
# We will round to 6 decimal places to avoid floating point errors
ROUND_DIGITS = 6


@default_retry
def _get_last_content_text() -> str:
    """Busca o texto bruto da curva de juros na ANBIMA."""
    request_payload = {
        "Idioma": "PT",
        "Dt_Ref": "",
        "saida": "csv",
    }
    r = requests.post(LAST_ETTJ_URL, data=request_payload)
    r.raise_for_status()
    r.encoding = "latin1"
    return r.text


def _get_reference_date(text: str) -> dt.date:
    file_date_str = text[0:10]  # formato = 09/09/2024
    file_date = dt.datetime.strptime(file_date_str, "%d/%m/%Y").date()
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
    return "\n".join(trecho_filtrado).replace(".", "").replace(",", ".")


def _convert_csv_to_df(text: str) -> pl.DataFrame:
    """Converte o texto CSV da curva de juros em um DataFrame Polars."""
    return pl.read_csv(StringIO(text), separator=";")


def _process_df(df: pl.DataFrame, reference_date: dt.date) -> pl.DataFrame:
    """Processa o DataFrame bruto, renomeando colunas e convertendo taxas."""
    # Rename columns
    rename_dict = {
        "Vertices": "vertex",
        "ETTJ IPCA": "real_rate",
        "ETTJ PREF": "nominal_rate",
        "Inflação Implícita": "implied_inflation",
    }
    rate_columns = ["real_rate", "nominal_rate", "implied_inflation"]
    df = df.rename(rename_dict).with_columns(
        pl.col(rate_columns).truediv(100).round(ROUND_DIGITS),
        date=reference_date,
    )
    column_order = [
        "date",
        "vertex",
        "nominal_rate",
        "real_rate",
        "implied_inflation",
    ]
    return df.select(column_order)


def last_ettj() -> pl.DataFrame:
    """Obtém e processa a última curva de juros (ETTJ) publicada pela ANBIMA.

    Busca os dados mais recentes da curva de juros de fechamento publicada pela
    ANBIMA, contendo taxas reais (indexadas ao IPCA), taxas nominais e inflação
    implícita em diversos vértices.

    Returns:
        pl.DataFrame: DataFrame com os dados da ETTJ de fechamento.

    Output Columns:
        * date (Date): data de referência da curva de juros.
        * vertex (Int64): vértice em dias úteis.
        * nominal_rate (Float64): taxa de juros nominal zero-cupom.
        * real_rate (Float64): taxa de juros real zero-cupom (indexada ao IPCA).
        * implied_inflation (Float64): taxa de inflação implícita (breakeven).

    Note:
        Todas as taxas são expressas em formato decimal (ex: 0.12 para 12%).
    """
    text = _get_last_content_text()
    reference_date = _get_reference_date(text)
    text = _filter_ettf_text(text)
    df = _convert_csv_to_df(text)
    df = _process_df(df, reference_date)
    return df
