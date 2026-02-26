import datetime as dt
import logging

import polars as pl
import polars.selectors as cs
import requests

import pyield._internal.converters as cv
from pyield import bday
from pyield._internal.types import DateLike, any_is_empty

VERSAO_API = "1.0018"
URL_BASE = (
    f"https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/{VERSAO_API}"
)

URL_PAGINA_INICIAL = f"{URL_BASE}/taxasOnline.asp"
URL_CONSULTA_DADOS = f"{URL_BASE}/exibedados.asp"
URL_DOWNLOAD = f"{URL_BASE}/download_dados.asp?extensao=csv"

MAPA_COLUNAS = {
    "Título": ("titulo", pl.String),
    "Vencimento": ("data_vencimento", pl.String),
    "Código ISIN": ("codigo_isin", pl.String),
    "Provedor": ("provedor", pl.String),
    "Edital": ("edital", pl.String),
    "Horário": ("horario", pl.String),
    "Prazo": ("prazo", pl.Int64),
    "Lote": ("lote", pl.String),
    "Fech D-1": ("taxa_indicativa_anterior", pl.Float64),
    "Indicativo Superior": ("taxa_limite_superior", pl.Float64),
    "Máxima": ("taxa_maxima", pl.Float64),
    "Média": ("taxa_media", pl.Float64),
    "Mínima": ("taxa_minima", pl.Float64),
    "Indicativo Inferior": ("taxa_limite_inferior", pl.Float64),
    "Última": ("taxa_ultima", pl.Float64),
    "Oferta Compra": ("taxa_compra", pl.Float64),
    "Oferta Venda": ("taxa_venda", pl.Float64),
    "Nº de Negócios": ("num_negocios", pl.Int64),
    "Quantidade Negociada": ("quantidade_negociada", pl.Int64),
    "Volume Negociado (R$)": ("volume_negociado", pl.Float64),
}

ESQUEMA_API = {col: dtype for col, (_alias, dtype) in MAPA_COLUNAS.items()}
ALIAS_COLUNAS = {col: alias for col, (alias, _dtype) in MAPA_COLUNAS.items()}

# Colunas não selecionadas estão vazias na API.
ORDEM_COLUNAS_FINAL = [
    "data_hora_referencia",
    "provedor",
    "titulo",
    "data_vencimento",
    "codigo_isin",
    "dias_uteis",
    "taxa_indicativa_anterior",
    "taxa_venda",
    "taxa_compra",
    "taxa_media",
    "taxa_ultima",
]

logger = logging.getLogger(__name__)


def _buscar_dados_url(data_referencia: str) -> str:
    cabecalhos = {
        "Referer": URL_PAGINA_INICIAL,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",  # noqa
        "Origin": "https://www.anbima.com.br",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    carga = {
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
        s.get(URL_PAGINA_INICIAL, headers=cabecalhos, timeout=60)
        try:
            response_consulta = s.post(
                URL_CONSULTA_DADOS, headers=cabecalhos, data=carga, timeout=60
            )
            response_consulta.raise_for_status()
            response_download = s.post(
                URL_DOWNLOAD, headers=cabecalhos, data=carga, timeout=60
            )
            response_download.raise_for_status()
            if "text/html" in response_download.headers.get("Content-Type", ""):
                raise ValueError(
                    "Servidor respondeu com HTML em vez de CSV para a data "
                    f"{data_referencia}."
                )
            response_download.encoding = "iso-8859-1"
            return response_download.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar dados para a data '{data_referencia}': {e}")
            raise


def _extrair_data_referencia_e_csv_limpo(csv_data: str) -> tuple[dt.date, bytes]:
    """Extrai a data de referência do CSV e devolve o conteúdo pronto para leitura."""
    linhas_csv = csv_data.strip().splitlines()
    if not linhas_csv:
        raise ValueError("CSV vazio ou inválido.")

    if ":" not in linhas_csv[0]:
        raise ValueError("Cabeçalho do CSV inválido: data de referência ausente.")

    data_ref_str = linhas_csv[0].split(":")[1].strip()
    data_ref = dt.datetime.strptime(data_ref_str, "%d/%m/%Y").date()

    # O CSV da Anbima contém ';' extra no fim do cabeçalho.
    # Remove apenas esse caractere para garantir a leitura correta.
    csv_limpo = (
        "\n".join(linhas_csv[1:])
        .replace("Volume Negociado (R$);", "Volume Negociado (R$)")
        .encode()
    )

    return data_ref, csv_limpo


def _processar_csv(data_ref: dt.date, csv_limpo: bytes) -> pl.DataFrame:
    """Converte o CSV limpo em um DataFrame Polars estruturado."""
    df = (
        pl.read_csv(
            csv_limpo,
            separator=";",
            decimal_comma=True,
            schema=ESQUEMA_API,
        )
        .rename(ALIAS_COLUNAS)
        .with_columns(cs.string().str.strip_chars())  # Remove espaços em branco
        .with_columns(
            pl.col("data_vencimento").str.to_date(format="%d/%m/%Y"),
            data_referencia=data_ref,  # Adiciona a coluna de data de referência
            # Ajusta o horário para 12:00:00 quando o provedor for "ANBIMA 12H"
            horario=pl.when(pl.col("provedor") == "ANBIMA 12H")
            .then(dt.time(12, 0))
            .otherwise(pl.col("horario").str.to_time(format="%H:%M:%S")),
            taxa_media=pl.mean_horizontal("taxa_compra", "taxa_venda"),
        )
        .with_columns(
            bday.count_expr("data_referencia", "data_vencimento").alias("dias_uteis"),
            # Converte as colunas de taxa de percentual para decimal
            # São 6 casas decimais no máximo.
            # Arredondar na 8a para minimizar erros de ponto flutuante.
            cs.starts_with("taxa_").truediv(100).round(8),
            pl.col("data_referencia")
            .dt.combine(pl.col("horario"))
            .dt.replace_time_zone("America/Sao_Paulo")
            .alias("data_hora_referencia"),
        )
        .select(ORDEM_COLUNAS_FINAL)
        .sort("titulo", "data_vencimento", "data_hora_referencia")
    )

    return df


def tpf_difusao(data_referencia: DateLike) -> pl.DataFrame:
    """
    Obtém a TPF Difusão da Anbima para uma data de referência específica.

    Args:
        data_referencia (DateLike): Data de referência.

    Returns:
        pl.DataFrame: DataFrame com os dados.

    Raises:
        requests.RequestException: Em falhas operacionais de rede ou HTTP.
        ValueError: Em resposta inválida da API ou erro de parsing do CSV.

    Output Columns:
        - data_hora_referencia (datetime[America/Sao_Paulo]): Data e hora de
            referência da taxa no fuso horário de São Paulo.
        - provedor (string): Provedor dos dados.
        - titulo (string): Nome do título (ex: LFT, LTN).
        - data_vencimento (date): Data de vencimento do título.
        - codigo_isin (string): Código ISIN do título.
        - dias_uteis (int): Dias úteis entre a data de referência e o vencimento.
        - taxa_indicativa_anterior (float): Taxa indicativa de fechamento D-1 (decimal).
        - taxa_venda (float): Taxa de oferta de venda (Ask rate) (decimal).
        - taxa_compra (float): Taxa de oferta de compra (Bid rate) (decimal).
        - taxa_media (float): Média entre a taxa de compra e venda (decimal).
        - taxa_ultima (float): Última taxa negociada (decimal).
    """
    if any_is_empty(data_referencia):
        logger.warning("Nenhuma data fornecida. Retornando DataFrame vazio.")
        return pl.DataFrame()
    data = cv.converter_datas(data_referencia)
    data_str = data.strftime("%d/%m/%Y")
    csv_data = _buscar_dados_url(data_str)

    data_ref, csv_limpo = _extrair_data_referencia_e_csv_limpo(csv_data)
    return _processar_csv(data_ref, csv_limpo)
