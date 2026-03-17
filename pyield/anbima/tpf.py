import datetime as dt
import logging
import socket
from typing import Literal

import polars as pl
import polars.selectors as cs
import requests
from requests.exceptions import HTTPError, RequestException

from pyield import bday
from pyield._internal.converters import converter_datas, data_referencia_valida
from pyield._internal.data_cache import obter_dataset_cacheado
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike

BOND_TYPES = Literal["LFT", "NTN-B", "NTN-C", "LTN", "NTN-F", "PRE"]

ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs"
ANBIMA_RTM_HOSTNAME = "www.anbima.associados.rtm"
ANBIMA_RTM_URL = f"http://{ANBIMA_RTM_HOSTNAME}/merc_sec/arqs"
# Exemplo de URL: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt

# Antes de 13/05/2014 o arquivo era zipado e o endpoint terminava com ".exe"
DATA_MUDANCA_FORMATO = dt.date(2014, 5, 13)

DIAS_RETENCAO_PUBLICA = 5

# Única fonte de verdade para colunas do CSV: (nome_csv, nome_novo, tipo)
# Colunas de data são lidas como String e convertidas em _processar_df_bruto
TPF_COLUNAS = [
    ("Titulo", "BondType", pl.String),
    ("Data Referencia", "ReferenceDate", pl.String),
    ("Codigo SELIC", "SelicCode", pl.Int64),
    ("Data Base/Emissao", "IssueBaseDate", pl.String),
    ("Data Vencimento", "MaturityDate", pl.String),
    ("Tx. Compra", "BidRate", pl.Float64),
    ("Tx. Venda", "AskRate", pl.Float64),
    ("Tx. Indicativas", "IndicativeRate", pl.Float64),
    ("PU", "Price", pl.Float64),
    ("Desvio padrao", "StdDev", pl.Float64),
    ("Interv. Ind. Inf. (D0)", "LowerBoundRateD0", pl.Float64),
    ("Interv. Ind. Sup. (D0)", "UpperBoundRateD0", pl.Float64),
    ("Interv. Ind. Inf. (D+1)", "LowerBoundRateD1", pl.Float64),
    ("Interv. Ind. Sup. (D+1)", "UpperBoundRateD1", pl.Float64),
    ("Criterio", "Criteria", pl.String),
]

# Derivados automaticamente
ESQUEMA_TPF = {csv: tipo for csv, _, tipo in TPF_COLUNAS}
MAPA_NOMES_COLUNAS = {csv: novo for csv, novo, _ in TPF_COLUNAS}

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
    ultimo_dia_util = bday.last_business_day()
    qtd_dias_uteis = bday.count(data, ultimo_dia_util)
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


def _ler_csv(csv_texto: bytes) -> pl.DataFrame:
    """
    Exemplo de arquivo bruto da ANBIMA:
        ANBIMA - Associação Brasileira das Entidades dos Mercados Financeiro e de Capitais

        Titulo@Data Referencia@Codigo SELIC@Data Base/Emissao@Data Vencimento@Tx. Compra@Tx. Venda@Tx. Indicativas@PU@Desvio padrao@Interv. Ind. Inf. (D0)@Interv. Ind. Sup. (D0)@Interv. Ind. Inf. (D+1)@Interv. Ind. Sup. (D+1)@Criterio
        LTN@20250924@100000@20230707@20251001@14,9483@14,9263@14,9375@997,241543@0,00433039162894@14,7341@15,2612@14,7316@15,2689@Calculado
        LTN@20250924@100000@20200206@20260101@14,7741@14,7485@14,7616@963,001853@0,00729826731971@14,7008@14,9986@14,7021@14,9975@Calculado
        LTN@20250924@100000@20240105@20260401@14,7357@14,707@14,7205@931,607124@0,00317937979329@14,5525@14,9847@14,5669@14,9959@Calculado
        ...
    """  # noqa
    df = pl.read_csv(
        source=csv_texto,
        skip_lines=2,
        separator="@",
        null_values=["--"],
        decimal_comma=True,
        schema_overrides=ESQUEMA_TPF,
        encoding="latin1",
    )
    return df


def _processar_df_bruto(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.rename(MAPA_NOMES_COLUNAS)
        .with_columns(
            # Remove o percentual das taxas
            # Colunas de taxa têm valores percentuais com 4 casas decimais
            # Arredonda para 6 casas decimais para minimizar erros de ponto flutuante
            cs.contains("Rate").truediv(100).round(6),
            cs.ends_with("Date").str.to_date(format="%Y%m%d"),
        )
        # Substituir eventuais NaNs por None para compatibilidade com bancos de dados
        .with_columns(cs.float().fill_nan(None))
    )
    return df


def _selecionar_e_ordenar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Reordena as colunas do DataFrame de acordo com a ordem especificada."""
    ordem_colunas = [
        "BondType",
        "ReferenceDate",
        "SelicCode",
        "IssueBaseDate",
        "MaturityDate",
        "Price",
        "BidRate",
        "AskRate",
        "IndicativeRate",
    ]
    ordem_colunas = [col for col in ordem_colunas if col in df.columns]
    return df.select(ordem_colunas).sort("BondType", "MaturityDate")


def _buscar_dados_tpf(date: dt.date) -> pl.DataFrame:
    """Busca e processa dados do mercado secundário de TPF diretamente da fonte ANBIMA.

    Esta é uma função de baixo nível para uso interno. Ela lida com a lógica
    de construir a URL correta (pública ou RTM), baixar os dados com novas
    tentativas e processá-los em um DataFrame estruturado.

    Args:
        date (dt.date): A data de referência para os dados.

    Returns:
        pl.DataFrame: Um DataFrame contendo os dados de mercado de títulos
            processados, ou um DataFrame vazio se os dados não estiverem
            disponíveis ou ocorrer um erro de conexão.
    """
    url_arquivo = _montar_url_arquivo(date)
    data_str = date.strftime("%d/%m/%Y")

    # --- "FAIL-FAST" PARA EVITAR RETRIES DESNECESSÁRIOS NA RTM ---
    if ANBIMA_RTM_URL in url_arquivo:
        try:
            # Tenta resolver o hostname da RTM. É uma verificação de rede rápida.
            socket.gethostbyname(ANBIMA_RTM_HOSTNAME)
        except socket.gaierror:
            # Se falhar (gaierror = get address info error), não estamos na RTM.
            # Não adianta prosseguir para a função com retry.
            logger.warning(
                f"Não foi possível resolver o host da RTM para {data_str}. "
                "Isso é esperado fora da rede RTM. Dados históricos exigem acesso "
                "à RTM. Retornando DataFrame vazio."
            )
            return pl.DataFrame()

    try:
        # Se passamos pela verificação da RTM, agora podemos chamar a função com retry.
        csv_texto = _obter_csv(date)
        if not csv_texto.strip():
            logger.info(
                f"Dados TPF de mercado secundário para {data_str} não disponíveis. "
                "Retornando DataFrame vazio."
            )
            return pl.DataFrame()

        df = _ler_csv(csv_texto)
        df = _processar_df_bruto(df)

        return df

    except HTTPError as e:
        if e.response.status_code == 404:  # noqa
            logger.info(
                f"Dados TPF de mercado secundário para {data_str} (HTTP 404). "
                "Retornando DataFrame vazio."
            )
            return pl.DataFrame()
        logger.error(
            "Erro HTTP ao buscar dados para %s de %s: %s", data_str, url_arquivo, e
        )
        raise

    # Este bloco ainda é útil para outros URLErrors (ex: timeout genuíno na URL pública)
    except RequestException:
        logger.exception("RequestException ao buscar dados TPF para %s", data_str)
        raise

    except Exception:
        msg = f"Ocorreu um erro inesperado ao buscar dados TPF para {data_str}"
        logger.exception(msg)
        raise


def tpf(
    date: DateLike,
    bond_type: BOND_TYPES | None = None,
) -> pl.DataFrame:
    """Recupera os dados do mercado secundário de TPF da ANBIMA.

    Esta função busca taxas indicativas e outros dados de títulos públicos
    brasileiros. Primeiro consulta o cache local; se não houver dados,
    busca diretamente na fonte (ANBIMA).

    Args:
        date (DateLike): A data de referência para os dados (ex: '2024-06-14').
        bond_type (str, optional): Filtra por tipo de título. Aceita os tipos
            individuais ('LTN', 'NTN-F', 'NTN-B', 'NTN-C', 'LFT') ou 'PRE'
            como atalho para prefixados ('LTN' e 'NTN-F').

    Returns:
        pl.DataFrame: Um DataFrame contendo os dados solicitados.
            Retorna um DataFrame vazio se não houver dados para a data especificada (ex:
            finais de semana, feriados ou datas futuras).

    Examples:
        >>> from pyield import anbima
        >>> df = anbima.tpf(date="06-02-2026")

    Data columns:
        - BondType: Tipo do título público (e.g., 'LTN', 'NTN-B').
        - ReferenceDate: Data de referência dos dados.
        - SelicCode: Código do título no SELIC.
        - IssueBaseDate: Data base ou de emissão do título.
        - MaturityDate: Data de vencimento do título.
        - Price: Preço Unitário (PU) calculado para liquidação em D0.
        - BidRate: Taxa de compra para liquidação em D0 (decimal).
        - AskRate: Taxa de venda para liquidação em D0 (decimal).
        - IndicativeRate: Taxa indicativa para liquidação em D0 (decimal).

    Notes:
        A fonte dos dados segue a seguinte hierarquia:

        1.  **Cache Local:** Fornece acesso rápido a dados históricos
            desde 01/01/2020.
        2.  **Fonte ANBIMA (fallback):** Se a data não estiver no cache,
            busca automaticamente na fonte. Para datas recentes (até 5 dias
            úteis), usa o site público. Para datas mais antigas, requer
            acesso à rede RTM.

        Para obter o dado completo direto da fonte (todas as colunas),
        use :func:`fetch_tpf`.
    """
    date = converter_datas(date)

    if not data_referencia_valida(date):
        return pl.DataFrame()

    # Cache primeiro; se não tiver, busca na fonte
    df = obter_dataset_cacheado("tpf")
    if not df.is_empty():
        df = df.filter(pl.col("ReferenceDate") == date)
    if df.is_empty():
        df = _buscar_dados_tpf(date)
    df = _selecionar_e_ordenar_colunas(df)

    if df.is_empty():
        return pl.DataFrame()

    if bond_type:
        norm_bond_type = _mapear_tipo_titulo(bond_type)
        df = df.filter(pl.col("BondType").is_in(norm_bond_type))

    return df.sort("ReferenceDate", "BondType", "MaturityDate")


def fetch_tpf(
    date: DateLike,
) -> pl.DataFrame:
    """Busca os dados do mercado secundário de TPF direto da fonte ANBIMA.

    Retorna todas as colunas publicadas pela ANBIMA, sem cache e sem
    filtro de colunas. Indicado para uso em jobs e pipelines de dados.

    Args:
        date (DateLike): Data de referência (ex: '2024-06-14').

    Returns:
        pl.DataFrame: DataFrame com todas as colunas da ANBIMA.
            Retorna DataFrame vazio se não houver dados.
    """
    date = converter_datas(date)

    if not data_referencia_valida(date):
        return pl.DataFrame()

    df = _buscar_dados_tpf(date)
    if df.is_empty():
        return pl.DataFrame()

    return df.sort("ReferenceDate", "BondType", "MaturityDate")


def tpf_maturities(
    date: DateLike,
    bond_type: BOND_TYPES,
) -> pl.Series:
    """Recupera os vencimentos existentes para um tipo de título na data especificada.

    Args:
        date (DateLike): A data de referência para os vencimentos.
        bond_type (BOND_TYPES): O tipo de título para filtrar (ex: 'PRE' para 'LTN'
            e 'NTN-F', ou especifique 'LTN' ou 'NTN-F' diretamente).

    Returns:
        pl.Series: Uma Series contendo as datas de vencimento únicas para o(s)
            tipo(s) de título especificado(s).

    Examples:
        >>> from pyield import anbima
        >>> anbima.tpf_maturities(date="22-08-2025", bond_type="PRE")
        shape: (18,)
        Series: 'MaturityDate' [date]
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
    return tpf(date, bond_type)["MaturityDate"].unique().sort()
