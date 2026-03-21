import datetime as dt
import logging
from io import StringIO

import polars as pl
import requests

from pyield._internal.br_numbers import numero_br, taxa_br
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

logger = logging.getLogger(__name__)

URL_ETTJ_INTRADAY = (
    "https://www.anbima.com.br/informacoes/curvas-intradiarias/cIntra-down.asp"
)

# Dados ETTJ: taxas com 4 casas decimais em percentual (ex.: 14,2644%).


@ttl_cache()
@retry_padrao
def _buscar_texto_intraday() -> str:
    carga_requisicao = {"Dt_Ref": "", "saida": "csv"}
    resposta = requests.post(URL_ETTJ_INTRADAY, data=carga_requisicao, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "latin1"
    return resposta.text


def _extrair_data_e_tabelas(texto: str) -> tuple[dt.date, str, str]:
    """Separa o texto bruto em data de referência e as duas tabelas CSV."""
    # Cada seção (PREFIXADOS e IPCA) é separada por linha em branco
    secao_pre, secao_ipca = texto.strip().replace("\r\n", "\n").split("\n\n")

    # Estrutura de cada seção: título, data, header+dados
    linhas_pre = secao_pre.splitlines()
    data_ref = dt.datetime.strptime(linhas_pre[1], "%d/%m/%Y").date()
    tabela_pre = "\n".join(linhas_pre[2:])

    linhas_ipca = secao_ipca.splitlines()
    tabela_ipca = "\n".join(linhas_ipca[2:])

    return data_ref, tabela_pre, tabela_ipca


def _parsear_tabela_intraday(texto: str, nome_taxa: str) -> pl.DataFrame:
    return pl.read_csv(StringIO(texto), separator=";", infer_schema=False).select(
        vertex=numero_br("Vertices").cast(pl.Int64),
        **{nome_taxa: taxa_br("D0")},
    )


def intraday_ettj() -> pl.DataFrame:
    """Obtém e processa a curva de juros intradiária da ANBIMA.

    Busca os dados mais recentes da curva de juros intradiária publicada pela
    ANBIMA, contendo taxas reais (indexadas ao IPCA), taxas nominais e inflação
    implícita em diversos vértices. A curva é publicada por volta das 12h30 BRT.

    Returns:
        pl.DataFrame: DataFrame com os dados intradiários da ETTJ.

    Output Columns:
        - date (Date): data de referência da curva de juros.
        - vertex (Int64): vértice em dias úteis.
        - nominal_rate (Float64): taxa de juros nominal zero-cupom.
        - real_rate (Float64): taxa de juros real zero-cupom (indexada ao IPCA).
        - implied_inflation (Float64): taxa de inflação implícita (breakeven).

    Note:
        Todas as taxas são expressas em formato decimal (ex: 0.12 para 12%).
    """
    texto_api = _buscar_texto_intraday()

    data_ref, tabela_pre, tabela_ipca = _extrair_data_e_tabelas(texto_api)

    df_pre = _parsear_tabela_intraday(tabela_pre, "nominal_rate")
    df_ipca = _parsear_tabela_intraday(tabela_ipca, "real_rate")
    expr_inflacao_impl = (pl.col("nominal_rate") + 1) / (pl.col("real_rate") + 1) - 1
    df = df_pre.join(df_ipca, on="vertex", how="right").with_columns(
        date=data_ref,
        implied_inflation=expr_inflacao_impl.round(6),
    )
    return df.select("date", "vertex", "nominal_rate", "real_rate", "implied_inflation")
