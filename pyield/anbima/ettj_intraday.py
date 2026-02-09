import datetime as dt
import logging
from io import StringIO

import polars as pl
import requests

from pyield.retry import retry_padrao

logger = logging.getLogger(__name__)

URL_ETTJ_INTRADAY = (
    "https://www.anbima.com.br/informacoes/curvas-intradiarias/cIntra-down.asp"
)

# Dados ETTJ têm 4 casas decimais em valores percentuais.
# Arredondamos para 6 casas para evitar erros de ponto flutuante.
CASAS_DECIMAIS = 6


@retry_padrao
def _buscar_texto_intraday() -> str:
    carga_requisicao = {"Dt_Ref": "", "saida": "csv"}
    resposta = requests.post(URL_ETTJ_INTRADAY, data=carga_requisicao, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "latin1"
    return resposta.text


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


def _extrair_data_e_tabelas(texto: str) -> tuple[dt.date, str, str]:
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


def _parsear_tabela_intraday(texto: str) -> pl.DataFrame:
    return pl.read_csv(StringIO(texto), separator=";").drop("Fechamento D -1")


def intraday_ettj() -> pl.DataFrame:
    """Obtém e processa a curva de juros intradiária da ANBIMA.

    Busca os dados mais recentes da curva de juros intradiária publicada pela
    ANBIMA, contendo taxas reais (indexadas ao IPCA), taxas nominais e inflação
    implícita em diversos vértices. A curva é publicada por volta das 12h30 BRT.

    Returns:
        pl.DataFrame: DataFrame com os dados intradiários da ETTJ.

    Output Columns:
        * date (Date): data de referência da curva de juros.
        * vertex (Int64): vértice em dias úteis.
        * nominal_rate (Float64): taxa de juros nominal zero-cupom.
        * real_rate (Float64): taxa de juros real zero-cupom (indexada ao IPCA).
        * implied_inflation (Float64): taxa de inflação implícita (breakeven).

    Note:
        Todas as taxas são expressas em formato decimal (ex: 0.12 para 12%).
    """
    texto_api = _buscar_texto_intraday()

    # --- Extração da Tabela 1: PREFIXADOS ---
    data_ref, tabela_pre, tabela_ipca = _extrair_data_e_tabelas(texto_api)

    df_pre = _parsear_tabela_intraday(tabela_pre)
    df_pre = df_pre.rename({"D0": "nominal_rate"})

    df_ipca = _parsear_tabela_intraday(tabela_ipca)
    df_ipca = df_ipca.rename({"D0": "real_rate"})

    df = df_pre.join(df_ipca, on="Vertices", how="right")
    df = df.rename({"Vertices": "vertex"})

    df = df.with_columns(
        # convertendo de % para decimal e arredondando
        pl.col("real_rate").truediv(100).round(CASAS_DECIMAIS),
        pl.col("nominal_rate").truediv(100).round(CASAS_DECIMAIS),
        date=data_ref,
    ).with_columns(
        ((pl.col("nominal_rate") + 1) / (pl.col("real_rate") + 1) - 1)
        .round(CASAS_DECIMAIS)
        .alias("implied_inflation"),
    )
    ordem_colunas = ["date", "vertex", "nominal_rate", "real_rate", "implied_inflation"]
    return df.select(ordem_colunas)
