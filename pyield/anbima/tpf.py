import datetime as dt
import logging
import socket
from typing import Literal

import polars as pl
import polars.selectors as cs
import requests
from requests.exceptions import HTTPError, RequestException

from pyield import bday, clock
from pyield.b3 import di1
from pyield.bc.ptax_api import ptax
from pyield.converters import converter_datas
from pyield.data_cache import get_cached_dataset
from pyield.retry import default_retry
from pyield.tn.ntnb import duration as duration_b
from pyield.tn.ntnc import duration as duration_c
from pyield.tn.ntnf import duration as duration_f
from pyield.types import DateLike, any_is_empty

BOND_TYPES = Literal["LFT", "NTN-B", "NTN-C", "LTN", "NTN-F", "PRE"]

ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs"
ANBIMA_RTM_HOSTNAME = "www.anbima.associados.rtm"
ANBIMA_RTM_URL = f"http://{ANBIMA_RTM_HOSTNAME}/merc_sec/arqs"
# Exemplo de URL: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt

# Antes de 13/05/2014 o arquivo era zipado e o endpoint terminava com ".exe"
DATA_MUDANCA_FORMATO = dt.date(2014, 5, 13)

DIAS_RETENCAO_PUBLICA = 5

# Única fonte de verdade para colunas do CSV: (novo_nome, tipo)
# Colunas de data são lidas como String e convertidas em _processar_df_bruto
TPF_COLUNAS = {
    "Titulo": ("BondType", pl.String),
    "Data Referencia": ("ReferenceDate", pl.String),
    "Codigo SELIC": ("SelicCode", pl.Int64),
    "Data Base/Emissao": ("IssueBaseDate", pl.String),
    "Data Vencimento": ("MaturityDate", pl.String),
    "Tx. Compra": ("BidRate", pl.Float64),
    "Tx. Venda": ("AskRate", pl.Float64),
    "Tx. Indicativas": ("IndicativeRate", pl.Float64),
    "PU": ("Price", pl.Float64),
    "Desvio padrao": ("StdDev", pl.Float64),
    "Interv. Ind. Inf. (D0)": ("LowerBoundRateD0", pl.Float64),
    "Interv. Ind. Sup. (D0)": ("UpperBoundRateD0", pl.Float64),
    "Interv. Ind. Inf. (D+1)": ("LowerBoundRateD1", pl.Float64),
    "Interv. Ind. Sup. (D+1)": ("UpperBoundRateD1", pl.Float64),
    "Criterio": ("Criteria", pl.String),
}

# Derivados automaticamente
ESQUEMA_TPF = {k: v[1] for k, v in TPF_COLUNAS.items()}
MAPA_NOMES_COLUNAS = {k: v[0] for k, v in TPF_COLUNAS.items()}

logger = logging.getLogger(__name__)


def _validar_data_nao_futura(data: dt.date):
    """Levanta ValueError se a data for no futuro."""
    if data > clock.today():
        data_log = data.strftime("%d/%m/%Y")
        msg = f"Não é possível processar dados para data futura ({data_log})."
        raise ValueError(msg)


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


@default_retry
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
        .with_columns(
            BDToMat=bday.count_expr("ReferenceDate", "MaturityDate"),
        )
    )
    return df


def _calcular_duracao_por_linha(linha: dict) -> float:
    """Função auxiliar que será aplicada a cada linha do struct."""
    # Mapeia o BondType para a função de duration correspondente
    # Isso torna a lógica dentro do lambda ainda mais limpa
    tipo_titulo = linha["BondType"]
    if tipo_titulo == "LTN":
        return linha["BDToMat"] / 252  # A lógica da LTN depende apenas do BDToMat

    funcoes_duracao = {
        "NTN-F": duration_f,
        "NTN-B": duration_b,
        "NTN-C": duration_c,
    }

    func_duracao = funcoes_duracao.get(tipo_titulo)  # Busca da função correta
    if func_duracao:
        return func_duracao(
            linha["ReferenceDate"],
            linha["MaturityDate"],
            linha["IndicativeRate"],
        )
    # Se o BondType não for reconhecido, retorna 0.0 (LFT ou outros)
    return 0.0


def _adicionar_duracao(df_input: pl.DataFrame) -> pl.DataFrame:
    """Adiciona a coluna 'Duration' ao DataFrame Polars de forma otimizada."""
    colunas_necessarias = [
        "BondType",
        "ReferenceDate",
        "MaturityDate",
        "IndicativeRate",
        "BDToMat",  # Necessário para LTN
    ]
    # Adiciona a coluna Duration
    df = df_input.with_columns(
        pl.struct(colunas_necessarias)
        .map_elements(_calcular_duracao_por_linha, return_dtype=pl.Float64)
        .alias("Duration")
    )
    return df


def _adicionar_dv01(df_input: pl.DataFrame, data_ref: dt.date) -> pl.DataFrame:
    """Adiciona as colunas de DV01 ao DataFrame."""
    expr_duracao_mod = pl.col("Duration") / (1 + pl.col("IndicativeRate"))
    df = df_input.with_columns(DV01=0.0001 * expr_duracao_mod * pl.col("Price"))

    # DV01 em USD
    try:
        taxa_ptax = ptax(date=data_ref)
        df = df.with_columns(DV01USD=pl.col("DV01") / taxa_ptax)
    except Exception as e:
        logger.error("Erro ao adicionar DV01 em USD: %s", e)
    return df


def _adicionar_taxa_di(df: pl.DataFrame, data_ref: dt.date) -> pl.DataFrame:
    """Adiciona a coluna de taxa DI ao DataFrame."""
    taxas_di = di1.interpolate_rates(
        dates=data_ref,
        expirations=df["MaturityDate"],
        extrapolate=True,
    )
    df = df.with_columns(DIRate=taxas_di)
    return df


def _selecionar_e_ordenar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Reordena as colunas do DataFrame de acordo com a ordem especificada."""
    ordem_colunas = [
        "BondType",
        "ReferenceDate",
        "SelicCode",
        "IssueBaseDate",
        "MaturityDate",
        "BDToMat",
        "Duration",
        "DV01",
        "DV01USD",
        "Price",
        "BidRate",
        "AskRate",
        "IndicativeRate",
        "DIRate",
        "StdDev",
        "LowerBoundRateD0",
        "UpperBoundRateD0",
        "LowerBoundRateD1",
        "UpperBoundRateD1",
        "Criteria",
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
        df = _adicionar_duracao(df)
        df = _adicionar_dv01(df, date)
        df = _adicionar_taxa_di(df, date)
        df = _selecionar_e_ordenar_colunas(df)
        # Substituir eventuais NaNs por None para compatibilidade com bancos de dados
        df = df.with_columns(cs.float().fill_nan(None))

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


def tpf_data(
    date: DateLike,
    bond_type: BOND_TYPES | None = None,
    fetch_from_source: bool = False,
) -> pl.DataFrame:
    """Recupera os dados do mercado secundário de TPF da ANBIMA.

    Esta função busca taxas indicativas e outros dados de títulos públicos
    brasileiros. A obtenção dos dados segue uma hierarquia de fontes para
    otimizar o desempenho e o acesso.

    Args:
        date (DateLike): A data de referência para os dados (ex: '2024-06-14').
        bond_type (str, optional): Filtra os resultados por um tipo de título
            específico (ex: 'LTN', 'NTN-B'). Por padrão, retorna todos os tipos.
        fetch_from_source (bool, optional): Se True, força a função a ignorar o
            cache e buscar os dados diretamente da fonte (ANBIMA).
            Padrão é False.

    Returns:
        pl.DataFrame: Um DataFrame contendo os dados solicitados.
            Retorna um DataFrame vazio se não houver dados para a data especificada (ex:
            finais de semana, feriados ou datas futuras).

    Examples:
        >>> from pyield import anbima
        >>> anbima.tpf_data(date="22-08-2025")
        shape: (49, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬───────────┬───────────┬────────────────┬──────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate   ┆ AskRate   ┆ IndicativeRate ┆ DIRate   │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---       ┆ ---       ┆ ---            ┆ ---      │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64       ┆ f64       ┆ f64            ┆ f64      │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪═══════════╪═══════════╪════════════════╪══════════╡
        │ 2025-08-22    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.000198  ┆ 0.0001    ┆ 0.000165       ┆ 0.14906  │
        │ 2025-08-22    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ -0.000053 ┆ -0.000156 ┆ -0.000116      ┆ 0.14843  │
        │ 2025-08-22    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ -0.000053 ┆ -0.000143 ┆ -0.000107      ┆ 0.1436   │
        │ 2025-08-22    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.000309  ┆ 0.000292  ┆ 0.000302       ┆ 0.138189 │
        │ 2025-08-22    ┆ LFT      ┆ 210100    ┆ 2000-07-01    ┆ … ┆ 0.000421  ┆ 0.000399  ┆ 0.000411       ┆ 0.134548 │
        │ …             ┆ …        ┆ …         ┆ …             ┆ … ┆ …         ┆ …         ┆ …              ┆ …        │
        │ 2025-08-22    ┆ NTN-F    ┆ 950199    ┆ 2016-01-15    ┆ … ┆ 0.139379  ┆ 0.139163  ┆ 0.139268       ┆ 0.13959  │
        │ 2025-08-22    ┆ NTN-F    ┆ 950199    ┆ 2018-01-05    ┆ … ┆ 0.134252  ┆ 0.134018  ┆ 0.13414        ┆ 0.1327   │
        │ 2025-08-22    ┆ NTN-F    ┆ 950199    ┆ 2020-01-10    ┆ … ┆ 0.13846   ┆ 0.138355  ┆ 0.13841        ┆ 0.13626  │
        │ 2025-08-22    ┆ NTN-F    ┆ 950199    ┆ 2022-01-07    ┆ … ┆ 0.139503  ┆ 0.139321  ┆ 0.139398       ┆ 0.13807  │
        │ 2025-08-22    ┆ NTN-F    ┆ 950199    ┆ 2024-01-05    ┆ … ┆ 0.140673  ┆ 0.140566  ┆ 0.140633       ┆ 0.13845  │
        └───────────────┴──────────┴───────────┴───────────────┴───┴───────────┴───────────┴────────────────┴──────────┘

    Data columns:
        - BondType: Tipo do título público (e.g., 'LTN', 'NTN-B').
        - ReferenceDate: Data de referência dos dados.
        - SelicCode: Código do título no SELIC.
        - IssueBaseDate: Data base ou de emissão do título.
        - MaturityDate: Data de vencimento do título.
        - BDToMat: Número de dias úteis entre a data de referência e o vencimento.
        - Duration: Macaulay Duration do título em anos.
        - DV01: Variação financeira no preço do título (em BRL) para uma
            mudança de 1 basis point (0,01%) na taxa de juros.
        - DV01USD: O mesmo que DV01, mas convertido para USD pela PTAX do dia.
        - Price: Preço Unitário (PU) do título na data de referência.
        - BidRate: Taxa de compra em formato decimal (e.g., 0.10 para 10%).
        - AskRate: Taxa de venda em formato decimal.
        - IndicativeRate: Taxa indicativa em formato decimal.
        - DIRate: Taxa DI interpolada (flatforward) no vencimento do título.
        - StdDev: Desvio padrão da taxa indicativa.
        - LowerBoundRateD0: Limite inferior do intervalo indicativo para D+0.
        - UpperBoundRateD0: Limite superior do intervalo indicativo para D+0.
        - LowerBoundRateD1: Limite inferior do intervalo indicativo para D+1.
        - UpperBoundRateD1: Limite superior do intervalo indicativo para D+1.
        - Criteria: Critério utilizado pela ANBIMA para o cálculo.

    Notes:
        A fonte dos dados segue a seguinte hierarquia:

        1.  **Cache Local (Padrão):** Fornece acesso rápido a dados históricos
            desde 01/01/2020. É utilizado por padrão (`fetch_from_source=False`).
        2.  **Site Público da ANBIMA:** Acessado quando `fetch_from_source=True`,
            disponibiliza os dados dos últimos 5 dias úteis.
        3.  **Rede RTM da ANBIMA:** Acessada quando `fetch_from_source=True` para
            datas com mais de 5 dias úteis. O acesso ao histórico completo
            requer uma conexão à rede RTM. Sem ela, a consulta para datas
            antigas retornará um DataFrame vazio.
    """  # noqa
    if any_is_empty(date):
        return pl.DataFrame()
    date = converter_datas(date)
    _validar_data_nao_futura(date)

    if fetch_from_source:
        # Tenta buscar os dados diretamente da fonte (ANBIMA)
        df = _buscar_dados_tpf(date)
    else:
        # Caso contrário, obtém os dados do cache local
        df = get_cached_dataset("tpf").filter(pl.col("ReferenceDate") == date)

    if df.is_empty():
        return pl.DataFrame()

    if bond_type:
        norm_bond_type = _mapear_tipo_titulo(bond_type)
        df = df.filter(pl.col("BondType").is_in(norm_bond_type))

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
    return tpf_data(date, bond_type)["MaturityDate"].unique().sort()
