import datetime as dt
import logging
from io import StringIO

import polars as pl
import requests

from pyield.retry import retry_padrao

logger = logging.getLogger(__name__)
URL_ULTIMA_ETTJ = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"

# Dados ETTJ têm 4 casas decimais em valores percentuais.
# Arredondamos para 6 casas para evitar erros de ponto flutuante.
CASAS_DECIMAIS = 6


@retry_padrao
def _buscar_texto_ultima_ettj() -> str:
    """Busca o texto bruto da curva de juros na ANBIMA."""
    carga_requisicao = {
        "Idioma": "PT",
        "Dt_Ref": "",
        "saida": "csv",
    }
    resposta = requests.post(URL_ULTIMA_ETTJ, data=carga_requisicao)
    resposta.raise_for_status()
    resposta.encoding = "latin1"
    return resposta.text


def _obter_data_referencia(texto: str) -> dt.date:
    data_str = texto[0:10]  # formato = 09/09/2024
    data_ref = dt.datetime.strptime(data_str, "%d/%m/%Y").date()
    return data_ref


def _filtrar_texto_ettf(texto_completo: str) -> str:
    # Definir os marcadores de início e fim
    marcador_inicio = "Vertices;ETTJ IPCA;ETTJ PREF;Inflação Implícita"
    marcador_fim = "PREFIXADOS (CIRCULAR 3.361)"

    # 2. Dividir o texto em uma lista de linhas
    linhas = texto_completo.strip().splitlines()

    # 3. Encontrar os índices das linhas de início e fim
    indice_inicio = linhas.index(marcador_inicio)
    indice_fim = linhas.index(marcador_fim)

    # 4. Fatiar a lista para extrair o trecho desejado
    trecho_filtrado = linhas[indice_inicio:indice_fim]

    # Remover linhas vazias que possam ter sido incluídas no final
    while trecho_filtrado and not trecho_filtrado[-1].strip():
        trecho_filtrado.pop()

    # 5. Juntar as linhas filtradas em um único texto e retornar
    return "\n".join(trecho_filtrado).replace(".", "").replace(",", ".")


def _converter_csv_para_df(texto: str) -> pl.DataFrame:
    """Converte o texto CSV da curva de juros em um DataFrame Polars."""
    return pl.read_csv(StringIO(texto), separator=";")


def _processar_df(df: pl.DataFrame, data_referencia: dt.date) -> pl.DataFrame:
    """Processa o DataFrame bruto, renomeando colunas e convertendo taxas."""
    # Rename columns
    mapa_renomeacao = {
        "Vertices": "vertex",
        "ETTJ IPCA": "real_rate",
        "ETTJ PREF": "nominal_rate",
        "Inflação Implícita": "implied_inflation",
    }
    colunas_taxa = ["real_rate", "nominal_rate", "implied_inflation"]
    df = df.rename(mapa_renomeacao).with_columns(
        pl.col(colunas_taxa).truediv(100).round(CASAS_DECIMAIS),
        date=data_referencia,
    )
    ordem_colunas = [
        "date",
        "vertex",
        "nominal_rate",
        "real_rate",
        "implied_inflation",
    ]
    return df.select(ordem_colunas)


def last_ettj() -> pl.DataFrame:
    """Obtém e processa a última curva de juros (ETTJ) publicada pela ANBIMA.

    Busca os dados mais recentes da curva de juros de fechamento publicada pela
    ANBIMA, contendo taxas reais (indexadas ao IPCA), taxas nominais e inflação
    implícita em diversos vértices.

    Returns:
        pl.DataFrame: DataFrame com os dados da ETTJ de fechamento.

    Output Columns:
        * date (Date): data de referência da curva de juros.
        * vertex (Int64): vértice em dias úteis.
        * nominal_rate (Float64): taxa de juros nominal zero-cupom.
        * real_rate (Float64): taxa de juros real zero-cupom (indexada ao IPCA).
        * implied_inflation (Float64): taxa de inflação implícita (breakeven).

    Note:
        Todas as taxas são expressas em formato decimal (ex: 0.12 para 12%).
    """
    texto = _buscar_texto_ultima_ettj()
    data_referencia = _obter_data_referencia(texto)
    texto = _filtrar_texto_ettf(texto)
    df = _converter_csv_para_df(texto)
    df = _processar_df(df, data_referencia)
    return df
