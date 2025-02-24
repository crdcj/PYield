"""
Documentação da API do BC
    https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/documentacao
Exemplo de chamada:
    https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='02-19-2025'&@dataFinalCotacao='02-23-2025'&$format=text/csv
    https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='01-01-2025'&@dataFinalCotacao='2026-01-01'&$format=text/csv"

Primeira data disponível: 28.11.1984
Última data disponível: data atual

"""

import io
import logging

import pandas as pd
import requests

from pyield import date_converter as dc
from pyield.date_converter import DateScalar

logger = logging.getLogger(__name__)

"""Dicionário com o mapeamento das colunas da API do BC para o DataFrame final
Chaves com comentário serão descartadas ao final do processamento
A ordem das chaves será a ordem das colunas no DataFrame final"""
COLUMN_MAPPING = {
    "dataHoraCotacao": "DateTime",
    "cotacaoCompra": "BuyRate",
    "cotacaoVenda": "SellRate",
}

BASE_API_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"


def _load_from_url(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return pd.read_csv(
        io.StringIO(response.text),
        dtype_backend="numpy_nullable",
        decimal=",",
        date_format="%Y-%m-%d %H:%M:%S.%f",
        parse_dates=["dataHoraCotacao"],
    )


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Remover colunas que não serão utilizadas
    keep_columns = [col for col in COLUMN_MAPPING.keys() if col in df.columns]
    return df[keep_columns].rename(columns=COLUMN_MAPPING)


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    df["Date"] = df["DateTime"].dt.normalize()
    df["Time"] = df["DateTime"].dt.time
    df["BuyRate"] = df["BuyRate"].round(4)
    df["SellRate"] = df["SellRate"].round(4)
    df["MidRate"] = (df["BuyRate"] + df["SellRate"]) / 2
    df["MidRate"] = df["MidRate"].round(5)
    return df


def _reorder_and_sort_columns(df: pd.DataFrame) -> pd.DataFrame:
    column_sequence = ["Date", "Time", "DateTime", "BuyRate", "SellRate", "MidRate"]
    return df[column_sequence].sort_values(by=["DateTime"]).reset_index(drop=True)


def _fetch_df_from_url(url: str) -> pd.DataFrame:
    try:
        df = _load_from_url(url)
        if df.empty:
            logging.warning("No data found for the specified period.")
            return pd.DataFrame()
        df = _rename_columns(df)
        df = _process_df(df)
        df = _reorder_and_sort_columns(df)
        return df
    except Exception:
        logger.exception("Error fetching auction data from BC API.")
        return pd.DataFrame()


def ptax(
    start: DateScalar | None = None,
    end: DateScalar | None = None,
) -> pd.DataFrame:
    """
    Disponível desde 28.11.1984, refere-se às taxas administradas até março de 1990 e às
    taxas livres a partir de então (Resolução 1690, de 18.3.1990). As taxas administradas
    são aquelas fixadas pelo Banco Central; a partir de março de 1992, essa taxa recebeu a
    denominação de taxa PTAX (fechamento). Até 30 de junho de 2011, as taxas livres
    correspondiam à média das taxas efetivas de operações no mercado interbancário,
    ponderada pelo volume de transações do dia. A partir de 1 de julho de 2011 (Circular
    3506, de 23.9.2010), a Ptax passou a corresponder à média aritmética das taxas obtidas
    em quatro consultas diárias aos dealers de câmbio e refletem a taxa negociada no
    momento de abertura da janela de consulta; o boletim de fechamento PTAX corresponde à
    média aritmética das taxas dos boletins do dia.

    **Consultas de Período:**
    - Para consultar dados de um intervalo, forneça as datas de `start` e `end`.
      Exemplo: `auctions(start='2024-10-20', end='2024-10-27')`
    - Se apenas `start` for fornecido, a API do BC retornará dados de leilão a partir
      da data de `start` **até a data mais recente disponível**.
      Exemplo: `auctions(start='2024-10-20')`
    - Se apenas `end` for fornecido, a API do BC retornará dados de leilão **desde a
      data mais antiga disponível até a data de `end`**.
      Exemplo: `auctions(end='2024-10-27')`

    **Série Histórica Completa:**
    - Para recuperar a série histórica completa de leilões (desde 12/11/2012 até o
      último dia útil), chame a função sem fornecer os parâmetros `start` e `end`.
      Exemplo: `auctions()`

    Busca dados de leilões da API do BC para as datas de início e fim especificadas,
    filtrando os resultados diretamente na API pelo tipo de leilão, se especificado.
    O comportamento da função em relação aos parâmetros `start` e `end` segue o padrão
    da API do Banco Central:
    - Se `start` for fornecido e `end` não, a função retorna dados de `start` até o fim.
    - Se `end` for fornecido e `start` não, a API retorna dados do início até `end`.
    - Se ambos `start` e `end` forem omitidos, a API retorna a série histórica completa.

    Os dados podem ser filtrados pelo tipo de leilão especificado ("Sell" ou "Buy").
    Leilões de "Sell" são aqueles em que o Tesouro Nacional vende títulos ao mercado.
    Leilões de "Buy" são aqueles em que o Tesouro Nacional compra títulos do mercado.

    Args:
        start (DateScalar, opcional): A data de início para a consulta dos leilões.
            Se `start` for fornecido e `end` for `None`, a API retornará dados de
            leilão a partir de `start` até a data mais recente disponível.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.
        end (DateScalar, opcional): A data de fim para a consulta de dados de leilão.
            Se `end` for fornecido e `start` for `None`, a API retornará dados de
            leilão desde a data mais antiga disponível até a data de `end`.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.

    Returns:
        pd.DataFrame: Um DataFrame contendo dados de leilões para o período e tipo
            especificados. Em caso de erro ao buscar os dados, um DataFrame vazio
            é retornado e uma mensagem de erro é registrada no log.

    Notes:
        O DataFrame possui as seguintes colunas:
            - Date: Data da cotação.
            - Time: Hora da cotação.
            - DateTime: Data e hora da cotação.
            - BuyRate: Taxa de compra.
            - SellRate: Taxa de venda.
            - MidRate: Taxa média entre a taxa de compra e venda.

    """

    if start:
        start = dc.convert_input_dates(start)
    else:
        start = pd.Timestamp("1984-11-28")

    if end:
        end = dc.convert_input_dates(end)
    else:
        end = pd.Timestamp.now().normalize()

    start_str = start.strftime("%m-%d-%Y")
    end_str = end.strftime("%m-%d-%Y")

    # Monta a URL da API com as datas de início e fim
    url = BASE_API_URL
    url += f"@dataInicial='{start_str}'"
    url += f"&@dataFinalCotacao='{end_str}'"
    url += "&$format=text/csv"  # Adiciona o formato CSV ao final

    return _fetch_df_from_url(url)
