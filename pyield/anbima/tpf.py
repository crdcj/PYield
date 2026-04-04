"""Dados do mercado secundário de Títulos Públicos Federais (TPF) da ANBIMA.

Exemplo de URL:
    https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt

Exemplo de dado bruto (CSV separado por @, encoding latin1):
    ANBIMA - Associação Brasileira das Entidades dos Mercados Financeiro e de Capitais

    Titulo@Data Referencia@Codigo SELIC@Data Base/Emissao@Data Vencimento@Tx. Compra@Tx. Venda@Tx. Indicativas@PU@Desvio padrao@Interv. Ind. Inf. (D0)@Interv. Ind. Sup. (D0)@Interv. Ind. Inf. (D+1)@Interv. Ind. Sup. (D+1)@Criterio
    LTN@20250924@100000@20230707@20251001@14,9483@14,9263@14,9375@997,241543@0,00433039162894@14,7341@15,2612@14,7316@15,2689@Calculado
    LTN@20250924@100000@20200206@20260101@14,7741@14,7485@14,7616@963,001853@0,00729826731971@14,7008@14,9986@14,7021@14,9975@Calculado
"""  # noqa: E501

import datetime as dt
import logging
import socket
from typing import Literal

import polars as pl
import requests

from pyield import dus
from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.converters import converter_datas, data_referencia_valida
from pyield._internal.data_cache import obter_dataset_cacheado
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike

TipoTPF = Literal["LFT", "NTN-B", "NTN-C", "LTN", "NTN-F", "PRE"]

ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs"
ANBIMA_RTM_HOSTNAME = "www.anbima.associados.rtm"
ANBIMA_RTM_URL = f"http://{ANBIMA_RTM_HOSTNAME}/merc_sec/arqs"

# Antes de 13/05/2014 o arquivo era zipado e o endpoint terminava com ".exe"
DATA_MUDANCA_FORMATO = dt.date(2014, 5, 13)

DIAS_RETENCAO_PUBLICA = 5

# Colunas selecionadas pela função pública tpf()
COLUNAS_TPF = (
    "titulo",
    "data_referencia",
    "codigo_selic",
    "data_base",
    "data_vencimento",
    "pu",
    "taxa_compra",
    "taxa_venda",
    "taxa_indicativa",
)

logger = logging.getLogger(__name__)


def _mapear_tipo_titulo(tipo_titulo: str) -> list[str]:
    tipo_titulo = tipo_titulo.upper()
    mapa_titulos = {
        "PRE": ["LTN", "NTN-F"],
        "NTNB": ["NTN-B"],
        "NTNC": ["NTN-C"],
        "NTNF": ["NTN-F"],
    }
    return mapa_titulos.get(tipo_titulo, [tipo_titulo])


def _montar_nome_arquivo(data: dt.date) -> str:
    data_url = data.strftime("%y%m%d")
    if data < DATA_MUDANCA_FORMATO:
        nome_arquivo = f"ms{data_url}.exe"
    else:
        nome_arquivo = f"ms{data_url}.txt"
    return nome_arquivo


def _montar_url_arquivo(data: dt.date) -> str:
    ultimo_dia_util = dus.ultimo_dia_util()
    qtd_dias_uteis = dus.contar(data, ultimo_dia_util)
    if qtd_dias_uteis > DIAS_RETENCAO_PUBLICA:
        # Para datas com mais de 5 dias úteis, apenas os dados da RTM estão disponíveis
        logger.info("Tentando buscar dados RTM para %s", data.strftime("%d/%m/%Y"))
        url_arquivo = f"{ANBIMA_RTM_URL}/{_montar_nome_arquivo(data)}"
    else:
        url_arquivo = f"{ANBIMA_URL}/{_montar_nome_arquivo(data)}"
    return url_arquivo


@retry_padrao
def _obter_csv(data: dt.date) -> bytes:
    url_arquivo = _montar_url_arquivo(data)
    resposta = requests.get(url_arquivo, timeout=10)
    resposta.raise_for_status()
    return resposta.content


def _parsear_df(csv_bytes: bytes) -> pl.DataFrame:
    """Converte bytes brutos do CSV da ANBIMA em DataFrame (tudo string)."""
    return pl.read_csv(
        source=csv_bytes,
        skip_lines=2,
        separator="@",
        null_values=["--"],
        infer_schema=False,
        encoding="latin1",
    )


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Renomeia, converte tipos e define a ordem das colunas."""
    return df.select(
        titulo=pl.col("Titulo"),
        data_referencia=pl.col("Data Referencia").str.to_date("%Y%m%d"),
        codigo_selic=pl.col("Codigo SELIC").cast(pl.Int64),
        data_base=pl.col("Data Base/Emissao").str.to_date("%Y%m%d"),
        data_vencimento=pl.col("Data Vencimento").str.to_date("%Y%m%d"),
        taxa_compra=taxa_br("Tx. Compra"),
        taxa_venda=taxa_br("Tx. Venda"),
        taxa_indicativa=taxa_br("Tx. Indicativas"),
        pu=float_br("PU"),
        desvio_padrao=float_br("Desvio padrao"),
        taxa_intervalo_inf_d0=taxa_br("Interv. Ind. Inf. (D0)"),
        taxa_intervalo_sup_d0=taxa_br("Interv. Ind. Sup. (D0)"),
        taxa_intervalo_inf_d1=taxa_br("Interv. Ind. Inf. (D+1)"),
        taxa_intervalo_sup_d1=taxa_br("Interv. Ind. Sup. (D+1)"),
        criterio=pl.col("Criterio"),
    )


def _buscar_dados_tpf(data: dt.date) -> pl.DataFrame:
    """Busca e processa dados do mercado secundário de TPF na fonte ANBIMA.

    Args:
        data: Data de referência.

    Returns:
        DataFrame processado ou DataFrame vazio se indisponível.
    """
    url_arquivo = _montar_url_arquivo(data)

    # Fail-fast: se a URL é RTM e o host não resolve, não adianta tentar
    if ANBIMA_RTM_URL in url_arquivo:
        try:
            socket.gethostbyname(ANBIMA_RTM_HOSTNAME)
        except socket.gaierror:
            data_str = data.strftime("%d/%m/%Y")
            logger.warning(
                f"Não foi possível resolver o host da RTM para {data_str}. "
                "Dados históricos exigem acesso à rede RTM."
            )
            return pl.DataFrame()

    csv_bytes = _obter_csv(data)
    if not csv_bytes.strip():
        return pl.DataFrame()

    df = _parsear_df(csv_bytes)
    return _processar_df(df)


def tpf(
    data: DateLike,
    titulo: TipoTPF | None = None,
) -> pl.DataFrame:
    """Recupera os dados do mercado secundário de TPF da ANBIMA.

    Esta função busca taxas indicativas e outros dados de títulos públicos
    brasileiros. Primeiro consulta o cache local; se não houver dados,
    busca diretamente na fonte (ANBIMA).

    Args:
        data (DateLike): A data da consulta para os dados
            (ex: '2024-06-14').
        titulo (str, optional): Filtra por tipo de título. Aceita os tipos
            individuais ('LTN', 'NTN-F', 'NTN-B', 'NTN-C', 'LFT') ou 'PRE'
            como atalho para prefixados ('LTN' e 'NTN-F').

    Returns:
        pl.DataFrame: Um DataFrame contendo os dados solicitados.
            Retorna um DataFrame vazio se não houver dados para a data especificada (ex:
            finais de semana, feriados ou datas futuras).

    Examples:
        >>> from pyield import anbima
        >>> df = anbima.tpf(data="06-02-2026")

    Output Columns:
        * titulo (String): tipo do título público (ex: 'LTN', 'NTN-B').
        * data_referencia (Date): data de referência dos dados.
        * codigo_selic (Int64): código do título no SELIC.
        * data_base (Date): data base ou de emissão do título.
        * data_vencimento (Date): data de vencimento do título.
        * pu (Float64): preço unitário (PU) para liquidação em D0.
        * taxa_compra (Float64): taxa de compra em D0 (decimal).
        * taxa_venda (Float64): taxa de venda em D0 (decimal).
        * taxa_indicativa (Float64): taxa indicativa em D0 (decimal).

    Notes:
        A fonte dos dados segue a seguinte hierarquia:

        1.  **Cache Local:** Fornece acesso rápido a dados históricos
            desde 01/01/2020.
        2.  **Fonte ANBIMA (fallback):** Se a data não estiver no cache,
            busca automaticamente na fonte. Para datas recentes (até 5 dias
            úteis), usa o site público. Para datas mais antigas, requer
            acesso à rede RTM.

        Para obter o dado completo direto da fonte (todas as colunas),
        use :func:`tpf_fonte`.
    """
    data = converter_datas(data)

    if not data_referencia_valida(data):
        return pl.DataFrame()

    # Cache primeiro; se não tiver, busca na fonte
    df = obter_dataset_cacheado("tpf")
    if not df.is_empty():
        df = df.filter(pl.col("data_referencia") == data)
    if df.is_empty():
        df = _buscar_dados_tpf(data)
    if df.is_empty():
        return pl.DataFrame()

    df = df.select(col for col in COLUNAS_TPF if col in df.columns)

    if titulo:
        tipos_titulo = _mapear_tipo_titulo(titulo)
        df = df.filter(pl.col("titulo").is_in(tipos_titulo))

    return df.sort("data_referencia", "titulo", "data_vencimento")


def tpf_fonte(
    data: DateLike,
) -> pl.DataFrame:
    """Busca os dados do mercado secundário de TPF direto da fonte ANBIMA.

    Retorna todas as colunas publicadas pela ANBIMA, sem cache e sem
    filtro de colunas. Indicado para uso em jobs e pipelines de dados.

    Args:
        data (DateLike): Data da consulta (ex: '2024-06-14').

    Returns:
        pl.DataFrame: DataFrame com todas as colunas da ANBIMA.
            Retorna DataFrame vazio se não houver dados.
    """
    data = converter_datas(data)

    if not data_referencia_valida(data):
        return pl.DataFrame()

    df = _buscar_dados_tpf(data)
    if df.is_empty():
        return pl.DataFrame()

    return df.sort("data_referencia", "titulo", "data_vencimento")


def tpf_vencimentos(
    data: DateLike,
    titulo: TipoTPF,
) -> pl.Series:
    """Recupera os vencimentos existentes para um tipo de título na data especificada.

    Args:
        data (DateLike): A data da consulta para os vencimentos.
        titulo (TipoTPF): O tipo de título para filtrar (ex: 'PRE' para 'LTN'
            e 'NTN-F', ou especifique 'LTN' ou 'NTN-F' diretamente).

    Returns:
        pl.Series: Uma Series contendo as datas de vencimento únicas para o(s)
            tipo(s) de título especificado(s).

    Examples:
        >>> from pyield import anbima
        >>> anbima.tpf_vencimentos(data="22-08-2025", titulo="PRE")
        shape: (18,)
        Series: 'data_vencimento' [date]
        [
            2025-10-01
            2026-01-01
            2026-04-01
            2026-07-01
            2026-10-01
            …
            2030-01-01
            2031-01-01
            2032-01-01
            2033-01-01
            2035-01-01
        ]
    """
    return tpf(data, titulo)["data_vencimento"].unique().sort()
