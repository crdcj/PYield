import datetime as dt
import io
import logging

import pandas as pd
import polars as pl
import polars.selectors as cs
import requests

from pyield import date_converter as dc
from pyield.date_converter import DateScalar

# --- 1. Centralização e Organização das Constantes ---
API_VERSION = "1.0018"
BASE_URL = (
    f"https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/{API_VERSION}"
)

URL_PAGINA_INICIAL = f"{BASE_URL}/taxasOnline.asp"
URL_CONSULTA_DADOS = f"{BASE_URL}/exibedados.asp"
URL_DOWNLOAD = f"{BASE_URL}/download_dados.asp?extensao=csv"

COLUMN_ALIASES = {
    "Título": "titulo",
    "Vencimento": "data_vencimento",
    "Código ISIN": "codigo_isin",
    "Provedor": "provedor",
    "Edital": "edital",
    "Horário": "horario",
    "Prazo": "prazo",
    "Lote": "lote",
    "Fech D-1": "taxa_indicativa_anterior",
    "Indicativo Superior": "taxa_limite_superior",
    "Máxima": "taxa_maxima",
    "Média": "taxa_media",
    "Mínima": "taxa_minima",
    "Indicativo Inferior": "taxa_limite_inferior",
    "Última": "taxa_ultima",
    "Oferta Compra": "taxa_compra",
    "Oferta Venda": "taxa_venda",
    "Nº de Negócios": "num_negocios",
    "Quantidade Negociada": "quantidade_negociada",
    "Volume Negociado (R$)": "volume_negociado",
}

# Colunas não selecionadas estão vazias na API.
FINAL_COLUMN_ORDER = [
    "data_referencia",
    "horario",
    "titulo",
    "data_vencimento",
    "codigo_isin",
    "provedor",
    # "edital",
    # "prazo",
    "lote",
    # "num_negocios",
    # "quantidade_negociada",
    # "volume_negociado",
    # "taxa_minima",
    # "taxa_media",
    # "taxa_maxima",
    "taxa_indicativa_anterior",
    "taxa_limite_inferior",
    "taxa_limite_superior",
    "taxa_venda",
    "taxa_compra",
    "taxa_media",
    "taxa_ultima",
]

logger = logging.getLogger(__name__)


def _fetch_url_data(data_referencia: str) -> str | None:
    headers = {
        "Referer": URL_PAGINA_INICIAL,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",  # noqa
        "Origin": "https://www.anbima.com.br",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    payload = {
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

    with requests.Session() as s:
        s.get(URL_PAGINA_INICIAL, headers=headers)
        try:
            response_consulta = s.post(
                URL_CONSULTA_DADOS, headers=headers, data=payload
            )
            response_consulta.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao registrar a data '{data_referencia}' na sessão: {e}")
            return None  # 3. Retornar None em caso de falha

        try:
            response_download = s.post(URL_DOWNLOAD, headers=headers, data=payload)
            response_download.raise_for_status()
            if "text/html" in response_download.headers.get("Content-Type", ""):
                logger.error(
                    "AVISO: O servidor respondeu com HTML em vez de CSV para '%s'.",
                    data_referencia,
                )
                return None
            response_download.encoding = "iso-8859-1"
            return response_download.text
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Erro durante o download para a data '{data_referencia}': {e}"
            )
            return None


def _process_csv_data(csv_data: str) -> pl.DataFrame:
    """Converte o CSV bruto em um DataFrame Polars limpo e estruturado."""
    csv_rows = csv_data.strip().splitlines()

    # Extrai a data de referência da primeira linha do arquivo
    data_ref_str = csv_rows[0].split(":")[1].strip()
    data_ref = dt.datetime.strptime(data_ref_str, "%d/%m/%Y").date()

    # Prepara o conteúdo do CSV para leitura
    # O arquivo CSV da Anbima contém um ';' extra no final do cabeçalho.
    # Esta linha remove o ';' da última coluna para garantir a leitura correta.
    cleaned_csv_content = "\n".join(csv_rows[1:]).replace(
        "Volume Negociado (R$);", "Volume Negociado (R$)"
    )

    df = (
        pl.read_csv(io.StringIO(cleaned_csv_content), separator=";", decimal_comma=True)
        .rename(COLUMN_ALIASES)
        .with_columns(pl.col(pl.String).str.strip_chars())  # Remove espaços em branco
        .with_columns(
            pl.col("data_vencimento").str.strptime(pl.Date, format="%d/%m/%Y"),
            pl.col("horario").str.strptime(pl.Time, format="%H:%M:%S"),
            data_referencia=data_ref,  # Adiciona a coluna de data de referência
            taxa_media=pl.mean_horizontal("taxa_compra", "taxa_venda"),
        )
        .select(FINAL_COLUMN_ORDER)
        # Converte as colunas de taxa de percentual para decimal
        # São 6 casas decimais no máximo.
        # Arredondar na 8a para minimizar erros de ponto flutuante.
        .with_columns(((cs.starts_with("taxa_") & cs.numeric()) / 100).round(8))
        .sort(by=["titulo", "data_vencimento", "provedor", "horario"])
    )
    return df


def tpf_difusao(data_referencia: DateScalar) -> pd.DataFrame:
    """
    Obtém a TPF Difusão da Anbima para uma data de referência específica.

    Args:
        data_referencia (str | dt.date | dt.datetime):
            Data de referência (ex: "DD/MM/AAAA").

    Returns:
        pd.DataFrame: DataFrame com os dados. Retorna um DataFrame vazio se
            não houver dados ou em caso de erro.

    Output Columns:
        * data_referencia (date): Data de referência da consulta.
        * horario (time): Horário da negociação ou indicação (HH:MM:SS).
        * titulo (string): Nome do título (ex: LFT, LTN).
        * data_vencimento (date): Data de vencimento do título.
        * codigo_isin (string): Código ISIN do título.
        * provedor (string): Provedor dos dados.
        * lote (string): Lote de negociação.
        * taxa_indicativa_anterior (float): Taxa indicativa de fechamento D-1 (decimal).
        * taxa_limite_inferior (float): Taxa limite inferior (decimal).
        * taxa_limite_superior (float): Taxa limite superior (decimal).
        * taxa_venda (float): Taxa de oferta de venda (Ask rate) (decimal).
        * taxa_compra (float): Taxa de oferta de compra (Bid rate) (decimal).
        * taxa_media (float): Média entre a taxa de compra e venda (decimal).
        * taxa_ultima (float): Última taxa negociada (decimal).
    """
    data_str = dc.convert_input_dates(data_referencia)
    csv_data = _fetch_url_data(data_str)

    if csv_data is None:
        logger.warning("Nenhum dado foi retornado para a data '%s'.", data_str)
        return pd.DataFrame()

    try:
        df = _process_csv_data(csv_data)
        return df.to_pandas(use_pyarrow_extension_array=True)
    except Exception as e:
        logger.error("Falha ao processar o CSV para a data '%s': %s", data_str, e)
        return pd.DataFrame()
