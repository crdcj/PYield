import datetime as dt
import io
import logging

import polars as pl
import polars.selectors as cs
import requests

import pyield.converters as cv
from pyield import bday
from pyield.types import DateLike, has_nullable_args

# --- 1. Centralização e Organização das Constantes ---
API_VERSION = "1.0018"
BASE_URL = (
    f"https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/{API_VERSION}"
)

URL_PAGINA_INICIAL = f"{BASE_URL}/taxasOnline.asp"
URL_CONSULTA_DADOS = f"{BASE_URL}/exibedados.asp"
URL_DOWNLOAD = f"{BASE_URL}/download_dados.asp?extensao=csv"

API_SCHEMA = {
    "Título": pl.String,
    "Vencimento": pl.String,
    "Código ISIN": pl.String,
    "Provedor": pl.String,
    "Edital": pl.String,
    "Horário": pl.String,
    "Prazo": pl.Int64,
    "Lote": pl.String,
    "Fech D-1": pl.Float64,
    "Indicativo Superior": pl.Float64,
    "Máxima": pl.Float64,
    "Média": pl.Float64,
    "Mínima": pl.Float64,
    "Indicativo Inferior": pl.Float64,
    "Última": pl.Float64,
    "Oferta Compra": pl.Float64,
    "Oferta Venda": pl.Float64,
    "Nº de Negócios": pl.Int64,
    "Quantidade Negociada": pl.Int64,
    "Volume Negociado (R$)": pl.Float64,
}

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
    "data_hora_referencia",
    "provedor",
    "titulo",
    "data_vencimento",
    "codigo_isin",
    "dias_uteis",
    # "edital",
    # "prazo",
    # "lote", # é sempre "P"
    # "num_negocios",
    # "quantidade_negociada",
    # "volume_negociado",
    # "taxa_minima",
    # "taxa_media",
    # "taxa_maxima",
    "taxa_indicativa_anterior",
    # "taxa_limite_inferior",
    # "taxa_limite_superior",
    "taxa_venda",
    "taxa_compra",
    "taxa_media",
    "taxa_ultima",
]

logger = logging.getLogger(__name__)


def _fetch_url_data(data_referencia: str) -> str:
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
        s.get(URL_PAGINA_INICIAL, headers=headers, timeout=60)
        try:
            response_consulta = s.post(
                URL_CONSULTA_DADOS, headers=headers, data=payload, timeout=60
            )
            response_consulta.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao registrar a data '{data_referencia}' na sessão: {e}")
            return ""

        try:
            response_download = s.post(
                URL_DOWNLOAD, headers=headers, data=payload, timeout=60
            )
            response_download.raise_for_status()
            if "text/html" in response_download.headers.get("Content-Type", ""):
                logger.error(
                    "AVISO: O servidor respondeu com HTML em vez de CSV para '%s'.",
                    data_referencia,
                )
                return ""
            response_download.encoding = "iso-8859-1"
            return response_download.text
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Erro durante o download para a data '{data_referencia}': {e}"
            )
            return ""


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
        pl.read_csv(
            io.StringIO(cleaned_csv_content),
            separator=";",
            decimal_comma=True,
            schema=API_SCHEMA,
        )
        .rename(COLUMN_ALIASES)
        .with_columns(pl.col(pl.String).str.strip_chars())  # Remove espaços em branco
        .with_columns(
            pl.col("data_vencimento").str.strptime(pl.Date, format="%d/%m/%Y"),
            data_referencia=data_ref,  # Adiciona a coluna de data de referência
            # Ajusta o horário para 12:00:00 quando o provedor for "ANBIMA 12H"
            horario=pl.when(pl.col("provedor") == "ANBIMA 12H")
            .then(pl.lit(dt.time(12, 0)))  # <-- Cria um literal do tipo Time
            .otherwise(pl.col("horario").str.strptime(pl.Time, format="%H:%M:%S")),
            taxa_media=pl.mean_horizontal("taxa_compra", "taxa_venda"),
        )
    )

    dias_uteis = bday.count(df["data_referencia"], df["data_vencimento"])
    df = (
        df.with_columns(
            pl.Series("dias_uteis", dias_uteis),
            # Converte as colunas de taxa de percentual para decimal
            # São 6 casas decimais no máximo.
            # Arredondar na 8a para minimizar erros de ponto flutuante.
            (cs.starts_with("taxa_") / 100).round(8),
            pl.col("data_referencia")
            .dt.combine(pl.col("horario"))
            .alias("data_hora_referencia"),
        )
        .select(FINAL_COLUMN_ORDER)
        .sort(by=["titulo", "data_vencimento", "data_hora_referencia"])
    )

    return df


def tpf_difusao(data_referencia: DateLike) -> pl.DataFrame:
    """
    Obtém a TPF Difusão da Anbima para uma data de referência específica.

    Args:
        data_referencia (str | dt.date | dt.datetime):
            Data de referência (ex: "DD/MM/AAAA").

    Returns:
        pl.DataFrame: DataFrame com os dados. Retorna um DataFrame vazio se
            não houver dados ou em caso de erro.

    Output Columns:
        * data_hora_referencia (datetime): Data e hora de referência da taxa.
        * provedor (string): Provedor dos dados.
        * titulo (string): Nome do título (ex: LFT, LTN).
        * data_vencimento (date): Data de vencimento do título.
        * codigo_isin (string): Código ISIN do título.
        * dias_uteis (int): Dias úteis entre a data de referência e o vencimento.
        * taxa_indicativa_anterior (float): Taxa indicativa de fechamento D-1 (decimal).
        * taxa_venda (float): Taxa de oferta de venda (Ask rate) (decimal).
        * taxa_compra (float): Taxa de oferta de compra (Bid rate) (decimal).
        * taxa_media (float): Média entre a taxa de compra e venda (decimal).
        * taxa_ultima (float): Última taxa negociada (decimal).
    """
    if has_nullable_args(data_referencia):
        logger.warning("Nenhuma data fornecida. Retornando DataFrame vazio.")
        return pl.DataFrame()
    data = cv.convert_dates(data_referencia)
    data_str = data.strftime("%d/%m/%Y")
    csv_data = _fetch_url_data(data_str)

    if not csv_data:
        logger.warning("Nenhum dado foi retornado para a data '%s'.", data_str)
        return pl.DataFrame()

    try:
        df = _process_csv_data(csv_data)
        return df
    except Exception as e:
        logger.error("Falha ao processar o CSV para a data '%s': %s", data_str, e)
        return pl.DataFrame()
