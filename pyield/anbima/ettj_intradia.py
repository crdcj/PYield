import datetime as dt

import polars as pl
import requests

from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

URL_ETTJ_INTRADIA = (
    "https://www.anbima.com.br/informacoes/curvas-intradiarias/cIntra-down.asp"
)

# Dados ETTJ: taxas com 4 casas decimais em percentual (ex.: 14,2644%).


@ttl_cache()
@retry_padrao
def _buscar_texto_intradia() -> str:
    carga_requisicao = {"Dt_Ref": "", "saida": "csv"}
    resposta = requests.post(URL_ETTJ_INTRADIA, data=carga_requisicao, timeout=10)
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


def _parsear_tabela_intradia(texto: str, nome_taxa: str) -> pl.DataFrame:
    return pl.read_csv(texto.encode(), separator=";", infer_schema=False).select(
        vertice=float_br("Vertices").cast(pl.Int64),
        **{nome_taxa: taxa_br("D0")},
    )


def ettj_intradia() -> pl.DataFrame:
    """Obtém e processa a curva de juros intradiária da ANBIMA.

    Busca os dados mais recentes da curva de juros intradiária publicada pela
    ANBIMA, contendo taxas reais (indexadas ao IPCA), taxas nominais e inflação
    implícita em diversos vértices. A curva é publicada por volta das 12h30 BRT.

    Returns:
        pl.DataFrame: DataFrame com os dados intradiários da ETTJ.

    Output Columns:
        - data_referencia (Date): data de referência da curva de juros.
        - vertice (Int64): vértice em dias úteis.
        - taxa_nominal (Float64): taxa de juros nominal zero-cupom.
        - taxa_real (Float64): taxa de juros real zero-cupom (indexada ao IPCA).
        - inflacao_implicita (Float64): taxa de inflação implícita (breakeven).

    Notes:
        Todas as taxas são expressas em formato decimal (ex: 0.12 para 12%).
    """
    texto_api = _buscar_texto_intradia()

    data_ref, tabela_pre, tabela_ipca = _extrair_data_e_tabelas(texto_api)

    df_pre = _parsear_tabela_intradia(tabela_pre, "taxa_nominal")
    df_ipca = _parsear_tabela_intradia(tabela_ipca, "taxa_real")
    expr_inflacao_impl = (pl.col("taxa_nominal") + 1) / (pl.col("taxa_real") + 1) - 1
    df = df_pre.join(df_ipca, on="vertice", how="right").with_columns(
        data_referencia=data_ref,
        inflacao_implicita=expr_inflacao_impl.round(6),
    )
    return df.select(
        "data_referencia", "vertice", "taxa_nominal", "taxa_real", "inflacao_implicita"
    )
