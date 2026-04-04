import datetime as dt

import polars as pl
import requests

from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

URL_ETTJ_ULTIMA = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"

# Dados ETTJ: taxas com 4 casas decimais em percentual (ex.: 14,2644%).


@ttl_cache()
@retry_padrao
def _buscar_texto_ettj_ultima() -> str:
    """Busca o texto bruto da curva de juros na ANBIMA."""
    carga_requisicao = {
        "Idioma": "PT",
        "Dt_Ref": "",
        "saida": "csv",
    }
    resposta = requests.post(URL_ETTJ_ULTIMA, data=carga_requisicao, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "latin1"
    return resposta.text


def _extrair_data_e_tabela(texto: str) -> tuple[dt.date, str]:
    """Separa o texto bruto em data de referência e a tabela CSV principal."""
    # Seções separadas por linha em branco: betas, ETTJ, circular, erros
    secoes = texto.strip().replace("\r\n", "\n").split("\n\n")
    # A data está na primeira linha da primeira seção (ex.: "20/03/2026;Beta 1;...")
    data_str = secoes[0].splitlines()[0][:10]
    data_ref = dt.datetime.strptime(data_str, "%d/%m/%Y").date()
    # A tabela ETTJ é a segunda seção; pular o título (primeira linha)
    linhas_ettj = secoes[1].splitlines()
    tabela = "\n".join(linhas_ettj[1:])
    return data_ref, tabela


def _processar_tabela(texto: str, data_referencia: dt.date) -> pl.DataFrame:
    """Lê o CSV e converte para DataFrame com taxas decimais."""
    return pl.read_csv(texto.encode(), separator=";", infer_schema=False).select(
        data_referencia=data_referencia,
        vertice=float_br("Vertices").cast(pl.Int64),
        taxa_nominal=taxa_br("ETTJ PREF"),
        taxa_real=taxa_br("ETTJ IPCA"),
        inflacao_implicita=taxa_br("Inflação Implícita"),
    )


def ettj_ultima() -> pl.DataFrame:
    """Obtém e processa a última curva de juros (ETTJ) publicada pela ANBIMA.

    Busca os dados mais recentes da curva de juros de fechamento publicada pela
    ANBIMA, contendo taxas reais (indexadas ao IPCA), taxas nominais e inflação
    implícita em diversos vértices.

    Returns:
        pl.DataFrame: DataFrame com os dados da ETTJ de fechamento.

    Output Columns:
        - data_referencia (Date): data de referência da curva de juros.
        - vertice (Int64): vértice em dias úteis.
        - taxa_nominal (Float64): taxa de juros nominal zero-cupom.
        - taxa_real (Float64): taxa de juros real zero-cupom (indexada ao IPCA).
        - inflacao_implicita (Float64): taxa de inflação implícita (breakeven).

    Notes:
        Todas as taxas são expressas em formato decimal (ex: 0.12 para 12%).
    """
    texto = _buscar_texto_ettj_ultima()
    data_ref, tabela = _extrair_data_e_tabela(texto)
    return _processar_tabela(tabela, data_ref)
