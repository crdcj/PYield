import datetime as dt
import io
import logging

import polars as pl
import requests

from pyield import date_converter as dc
from pyield.date_converter import DateScalar

COLUMN_ALIASES = {
    "Título": "titulo",
    "Vencimento": "data_vencimento",
    "Código ISIN": "codigo_isin",
    "Provedor": "provedor",
    "Edital": "edital",
    "Horário": "horario",
    "Prazo": "prazo",
    "Lote": "lote",
    "Fech D-1": "taxa_indicativa_d1",
    "Indicativo Superior": "taxa_limite_superior",
    "Máxima": "taxa_maxima",
    "Média": "taxa_media",
    "Mínima": "taxa_minima",
    "Indicativo Inferior": "taxa_limite_inferior",
    "Última": "taxa_ultima",
    "Oferta Compra": "taxa_bid",
    "Oferta Venda": "taxa_ask",
    "Nº de Negócios": "num_negocios",
    "Quantidade Negociada": "quantidade_negociada",
    "Volume Negociado (R$)": "volume_negociado",
}

logger = logging.getLogger(__name__)

url_pagina_inicial = "https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/1.0018/taxasOnline.asp"
url_consulta_dados = "https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/1.0018/exibedados.asp"
url_download = "https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/1.0018/download_dados.asp?extensao=csv"


def _fetch_url_data(data_referencia) -> str:
    headers = {  # Cabeçalhos para simular um navegador real
        # O Referer precisa ser a página de onde a ação se origina.
        "Referer": url_pagina_inicial,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",  # noqa
        "Origin": "https://www.anbima.com.br",
        # Necessário para POST com payload
        "Content-Type": "application/x-www-form-urlencoded",
    }

    payload = {  # Payload com a data. Usaremos para a consulta e o download.
        "dataref": data_referencia,
        "dtRefIdioma": data_referencia,
        "layoutimprimir": "0",
        "idioma": "",
        "idresumo": "",
        "nome": "",
        "provedor": "",
        "codigo": "",
        "vencimento": "",
        "referencia": "",
        "dbNome": "",
        "fldColunas": ["C2", "C17", "C18", "C19"],
    }

    # 1. Iniciar a sessão
    with requests.Session() as s:
        # P1: Fazer um GET na página inicial para obter os cookies de sessão.
        s.get(url_pagina_inicial, headers=headers)

        # P2: Simular o clique em "Consultar" enviando o POST para o endpoint correto.
        try:
            response_consulta = s.post(
                url_consulta_dados, headers=headers, data=payload
            )
            response_consulta.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao registrar a data na sessão: {e}")
            return ""

        # P3: Com a data devidamente registrada na sessão, solicitar o download.
        try:
            response_download = s.post(url_download, headers=headers, data=payload)
            response_download.raise_for_status()

            # Checando se o conteúdo parece ser um CSV e não uma página de erro
            if "text/html" in response_download.headers.get("Content-Type", ""):
                logger.error("AVISO: O servidor respondeu com HTML em vez de CSV.")
                logger.error(f"Conteúdo recebido: {response_download.text[:500]}")
                return ""
            response_download.encoding = "iso-8859-1"
            return response_download.text

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro durante o download: {e}")
            if "response_download" in locals():
                logger.error(f"Resposta do servidor: {response_download.text[:500]}")
            return ""


def _convert_to_df(csv_data: str) -> pl.DataFrame:
    csv_rows = csv_data.splitlines()
    data_ref_str = csv_rows[0].split(":")[1].strip()
    data_ref = dt.datetime.strptime(data_ref_str, "%d/%m/%Y").date()
    cleaned_csv_data = "\n".join(csv_rows[1:]).replace(
        "Volume Negociado (R$);", "Volume Negociado (R$)"
    )
    df = pl.read_csv(
        io.StringIO(cleaned_csv_data), separator=";", decimal_comma=True
    ).with_columns(
        pl.col(pl.String).str.strip_chars(),
        data_referencia=data_ref,
    )
    return df


def _process_df(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename(COLUMN_ALIASES).with_columns(
        pl.col("data_vencimento").str.strptime(pl.Date, format="%d/%m/%Y"),
        pl.col("horario").str.strptime(pl.Time, format="%H:%M:%S"),
        taxa_mid=pl.mean_horizontal("taxa_bid", "taxa_ask"),
    )
    return df


def _reorder_columns(df: pl.DataFrame):
    columns_order = [
        "data_referencia",
        "horario",
        "titulo",
        "data_vencimento",
        "codigo_isin",
        "provedor",
        "edital",
        "prazo",
        "lote",
        "num_negocios",
        "quantidade_negociada",
        "volume_negociado",
        "taxa_indicativa_d1",
        "taxa_minima",
        "taxa_media",
        "taxa_maxima",
        "taxa_ultima",
        "taxa_limite_superior",
        "taxa_limite_inferior",
        "taxa_bid",
        "taxa_ask",
        "taxa_mid",
    ]
    return df.select(columns_order)


def tpf_difusao(data_referencia: DateScalar) -> pl.DataFrame:
    """
    Obtém a TPF Difusão da Anbima para uma data de referência específica.

    Parâmetros:
    -----------
    data_referencia : str
        Data de referência no formato "DD/MM/AAAA".

    Retorna:
    --------
    pl.DataFrame
        DataFrame contendo os dados da TPF Difusão.
    """
    data_referencia = dc.convert_input_dates(data_referencia)
    csv_data = _fetch_url_data(data_referencia)
    if not csv_data:
        logger.error("Nenhum dado foi retornado para a data fornecida.")
        return pl.DataFrame()

    df = _convert_to_df(csv_data)
    df = _process_df(df)
    df = _reorder_columns(df)
    return df
